/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.FileFilter;
import java.io.IOException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import javax.management.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.commons.collections.PredicateUtils;

public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    private int minimumCompactionThreshold = 4; // compact this many sstables min at a time
    private int maximumCompactionThreshold = 32; // compact this many sstables max at a time

    static
    {
        instance = new CompactionManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private CompactionExecutor executor = new CompactionExecutor();
    private Map<ColumnFamilyStore, Integer> estimatedCompactions = new NonBlockingHashMap<ColumnFamilyStore, Integer>();

    /** cleans up data files for CFs that have been dropped. */
    public Future submitGraveyardCleanup()
    {
        Callable c =  new Callable()
        {
            public Object call() throws Exception
            {
                logger.debug("Cleaning up abandoned column families...");
                ColumnFamily dropped = SystemTable.getDroppedCFs();
                Collection<IColumn> successes = new ArrayList<IColumn>();
                for (IColumn col : dropped.getSortedColumns())
                {
                    if (!col.isMarkedForDelete())
                    {
                        final String[] parts = new String(col.name()).split("-");
                        // table-cfname-cfid
                        for (String dataDir : DatabaseDescriptor.getAllDataFileLocationsForTable(parts[0]))
                        {
                            File dir = new File(dataDir);
                            if (dir.exists())
                            {
                                File[] dbFiles = dir.listFiles(new FileFilter()
                                {
                                    public boolean accept(File pathname)
                                    {
                                        return pathname.getName().startsWith(parts[1] + "-") && pathname.exists();
                                    }
                                });
                                for (File f : dbFiles)
                                {
                                    FileUtils.deleteWithConfirm(f);
                                }
                            }
                        }
                        successes.add(col);
                    }
                }
                SystemTable.deleteDroppedCfMarkers(successes);
                return null;
            }
        };
        return executor.submit(c);
    }
    
    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public Future<Integer> submitMinorIfNeeded(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                if (minimumCompactionThreshold <= 0 || maximumCompactionThreshold <= 0)
                {
                    logger.debug("Compaction is currently disabled.");
                    return 0;
                }
                logger.debug("Checking to see if compaction of " + cfs.columnFamily_ + " would be useful");
                Set<List<SSTableReader>> buckets = getBuckets(cfs.getSSTables(), 50L * 1024L * 1024L);
                updateEstimateFor(cfs, buckets);
                
                for (List<SSTableReader> sstables : buckets)
                {
                    if (sstables.size() >= minimumCompactionThreshold)
                    {
                        // if we have too many to compact all at once, compact older ones first -- this avoids
                        // re-compacting files we just created.
                        Collections.sort(sstables);
                        return doCompaction(cfs, sstables.subList(0, Math.min(sstables.size(), maximumCompactionThreshold)), getDefaultGCBefore());
                    }
                }
                return 0;
            }
        };
        return executor.submit(callable);
    }

    private void updateEstimateFor(ColumnFamilyStore cfs, Set<List<SSTableReader>> buckets)
    {
        int n = 0;
        for (List<SSTableReader> sstables : buckets)
        {
            if (sstables.size() >= minimumCompactionThreshold)
            {
                n += 1 + sstables.size() / (maximumCompactionThreshold - minimumCompactionThreshold);
            }
        }
        estimatedCompactions.put(cfs, n);
    }

    public Future<Object> submitCleanup(final ColumnFamilyStore cfStore)
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                doCleanupCompaction(cfStore);
                return this;
            }
        };
        return executor.submit(runnable);
    }

    public Future<List<SSTableReader>> submitAnticompaction(final ColumnFamilyStore cfStore, final Collection<Range> ranges, final InetAddress target)
    {
        Callable<List<SSTableReader>> callable = new Callable<List<SSTableReader>>()
        {
            public List<SSTableReader> call() throws IOException
            {
                return doAntiCompaction(cfStore, cfStore.getSSTables(), ranges, target);
            }
        };
        return executor.submit(callable);
    }

//TODO: TEST (AES support for Counter CFs)
    public Future<List<SSTableReader>> submitAESCompaction(final ColumnFamilyStore cfStore, final Collection<Range> ranges, final InetAddress target)
    {
        Callable<List<SSTableReader>> callable = new Callable<List<SSTableReader>>()
        {
            public List<SSTableReader> call() throws IOException
            {
                return doAESCompaction(cfStore, cfStore.getSSTables(), ranges, target);
            }
        };
        return executor.submit(callable);
    }

    public Future submitMajor(final ColumnFamilyStore cfStore)
    {
        return submitMajor(cfStore, 0, getDefaultGCBefore());
    }

    public Future submitMajor(final ColumnFamilyStore cfStore, final long skip, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                Collection<SSTableReader> sstables;
                if (skip > 0)
                {
                    sstables = new ArrayList<SSTableReader>();
                    for (SSTableReader sstable : cfStore.getSSTables())
                    {
                        if (sstable.length() < skip * 1024L * 1024L * 1024L)
                        {
                            sstables.add(sstable);
                        }
                    }
                }
                else
                {
                    sstables = cfStore.getSSTables();
                }

                doCompaction(cfStore, sstables, gcBefore);
                return this;
            }
        };
        return executor.submit(callable);
    }

    public Future submitReadonly(final ColumnFamilyStore cfStore, final InetAddress initiator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                doReadonlyCompaction(cfStore, initiator);
                return this;
            }
        };
        return executor.submit(callable);
    }

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold()
    {
        return minimumCompactionThreshold;
    }

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold)
    {
        minimumCompactionThreshold = threshold;
    }

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold()
    {
        return maximumCompactionThreshold;
    }

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold)
    {
        maximumCompactionThreshold = threshold;
    }

    public void disableAutoCompaction()
    {
        minimumCompactionThreshold = 0;
        maximumCompactionThreshold = 0;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     */
    int doCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        Table table = cfs.getTable();
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            table.snapshot("compact-" + cfs.columnFamily_);
        logger.info("Compacting [" + StringUtils.join(sstables, ",") + "]");
        String compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(sstables));
        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        List<SSTableReader> smallerSSTables = new ArrayList<SSTableReader>(sstables);
        while (compactionFileLocation == null && smallerSSTables.size() > 1)
        {
            logger.warn("insufficient space to compact all requested files " + StringUtils.join(smallerSSTables, ", "));
            smallerSSTables.remove(cfs.getMaxSizeFile(smallerSSTables));
            compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(smallerSSTables));
        }
        if (compactionFileLocation == null)
        {
            logger.error("insufficient space to compact even the two smallest files, aborting");
            return 0;
        }
        sstables = smallerSSTables;

        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        boolean major = cfs.isCompleteSSTables(sstables);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        // TODO the int cast here is potentially buggy
        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)SSTableReader.getApproximateKeyCount(sstables));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer;
        CompactionIterator ci = new CompactionIterator(sstables, gcBefore, major); // retain a handle so we can call close()
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs, ci);

        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(sstables);
                return 0;
            }

            String newFilename = new File(cfs.getTempSSTablePath(compactionFileLocation)).getAbsolutePath();
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());

            // validate the CF as we iterate over it
            AntiEntropyService.IValidator validator = AntiEntropyService.instance.getValidator(table.name, cfs.getColumnFamilyName(), null, major);
            validator.prepare();
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                long prevpos = writer.getFilePointer();

                writer.append(row.key, row.buffer);
                validator.add(row);
                totalkeysWritten++;

                long rowsize = writer.getFilePointer() - prevpos;
                if (rowsize > DatabaseDescriptor.getRowWarningThreshold())
                    logger.warn("Large row " + row.key.key + " in " + cfs.getColumnFamilyName() + " " + rowsize + " bytes");
                cfs.addToCompactedRowStats(rowsize);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }

        SSTableReader ssTable = writer.closeAndOpenReader(getMaxDataAge(sstables));
        cfs.replaceCompactedSSTables(sstables, Arrays.asList(ssTable));
        submitMinorIfNeeded(cfs);

        String format = "Compacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
        long dTime = System.currentTimeMillis() - startTime;
        logger.info(String.format(format, writer.getFilename(), SSTable.getTotalBytes(sstables), ssTable.length(), totalkeysWritten, dTime));
        return sstables.size();
    }

    private static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }

    private String getCompactionFileLocation(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, InetAddress target)
            throws IOException
    {
        Table table = cfs.getTable();
        logger.info("AntiCompacting [" + StringUtils.join(sstables, ",") + "]");
        // Calculate the expected compacted filesize
        long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(sstables) / 2;
        String compactionFileLocation = table.getDataFileLocation(expectedRangeFileSize);
        if (compactionFileLocation == null)
        {
            throw new UnsupportedOperationException("disk full");
        }

        return compactionFileLocation;
    }

    private List<SSTableReader> performCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, String compactionFileLocation, CompactionIterator ci)
        throws IOException
    {
        List<SSTableReader> results = new ArrayList<SSTableReader>();
        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)(SSTableReader.getApproximateKeyCount(sstables) / 2));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer = null;
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs, ci);

        try
        {
            if (!nni.hasNext())
            {
                return results;
            }

            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                if (writer == null)
                {
                    FileUtils.createDirectory(compactionFileLocation);
                    String newFilename = new File(cfs.getTempSSTablePath(compactionFileLocation)).getAbsolutePath();
                    writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());
                }
                writer.append(row.key, row.buffer);
                totalkeysWritten++;
            }
        }
        finally
        {
            ci.close();
        }

        if (writer != null)
        {
            results.add(writer.closeAndOpenReader(getMaxDataAge(sstables)));
            String format = "AntiCompacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
            long dTime = System.currentTimeMillis() - startTime;
            logger.info(String.format(format, writer.getFilename(), SSTable.getTotalBytes(sstables), results.get(0).length(), totalkeysWritten, dTime));
        }

        return results;
    }

    /**
     * This function is used to do the anti compaction process , it spits out the file which has keys that belong to a given range
     * If the target is not specified it spits out the file as a compacted file with the unecessary ranges wiped out.
     *
     * @param cfs
     * @param sstables
     * @param ranges
     * @param target
     * @return
     * @throws java.io.IOException
     */
    private List<SSTableReader> doAntiCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        String compactionFileLocation = getCompactionFileLocation(cfs, sstables, target);
        return performCompaction(
            cfs,
            sstables,
            compactionFileLocation,
            new AntiCompactionIterator(sstables, ranges, getDefaultGCBefore(), cfs.isCompleteSSTables(sstables)));
    }

//TODO: TEST (AES support for Counter CFs)
    private List<SSTableReader> doAESCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        String compactionFileLocation = getCompactionFileLocation(cfs, sstables, target);

        ClockType clockType = DatabaseDescriptor.getClockType(cfs.getTable().name, cfs.getColumnFamilyName());
        if (!clockType.isContext()) {
            return performCompaction(
                cfs,
                sstables,
                compactionFileLocation,
                new AntiCompactionIterator(sstables, ranges, getDefaultGCBefore(), cfs.isCompleteSSTables(sstables)));
        } else {
            return performCompaction(
                cfs,
                sstables,
                compactionFileLocation,
                new CounterAESCompactionIterator(sstables, ranges, getDefaultGCBefore(), cfs.isCompleteSSTables(sstables), target));
        }
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(ColumnFamilyStore cfs) throws IOException
    {
        Collection<SSTableReader> originalSSTables = cfs.getSSTables();
        List<SSTableReader> sstables = doAntiCompaction(cfs, originalSSTables, StorageService.instance.getLocalRanges(cfs.getTable().name), null);
        if (!sstables.isEmpty())
        {
            cfs.replaceCompactedSSTables(originalSSTables, sstables);
        }
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    private void doReadonlyCompaction(ColumnFamilyStore cfs, InetAddress initiator) throws IOException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionIterator ci = new CompactionIterator(sstables, getDefaultGCBefore(), true);
        executor.beginCompaction(cfs, ci);
        try
        {
            Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

            // validate the CF as we iterate over it
            AntiEntropyService.IValidator validator = AntiEntropyService.instance.getValidator(cfs.getTable().name, cfs.getColumnFamilyName(), initiator, true);
            validator.prepare();
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }
    }

    /*
    * Group files of similar size into buckets.
    */
    static Set<List<SSTableReader>> getBuckets(Iterable<SSTableReader> files, long min)
    {
        Map<List<SSTableReader>, Long> buckets = new HashMap<List<SSTableReader>, Long>();
        for (SSTableReader sstable : files)
        {
            long size = sstable.length();

            boolean bFound = false;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `min`)
            for (Entry<List<SSTableReader>, Long> entry : buckets.entrySet())
            {
                List<SSTableReader> bucket = entry.getKey();
                long averageSize = entry.getValue();
                if ((size > averageSize / 2 && size < 3 * averageSize / 2)
                    || (size < min && averageSize < min))
                {
                    // remove and re-add because adding changes the hash
                    buckets.remove(bucket);
                    long totalSize = bucket.size() * averageSize;
                    averageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(sstable);
                    buckets.put(bucket, averageSize);
                    bFound = true;
                    break;
                }
            }
            // no similar bucket found; put it in a new one
            if (!bFound)
            {
                ArrayList<SSTableReader> bucket = new ArrayList<SSTableReader>();
                bucket.add(sstable);
                buckets.put(bucket, size);
            }
        }

        return buckets.keySet();
    }

    public static int getDefaultGCBefore()
    {
        return (int)(System.currentTimeMillis() / 1000) - DatabaseDescriptor.getGcGraceInSeconds();
    }

    private static class AntiCompactionIterator extends CompactionIterator
    {
        private Set<SSTableScanner> scanners;

        public AntiCompactionIterator(Collection<SSTableReader> sstables, Collection<Range> ranges, int gcBefore, boolean isMajor)
                throws IOException
        {
            super(getCollatedRangeIterator(sstables, ranges), gcBefore, isMajor);
        }

        private static Iterator getCollatedRangeIterator(Collection<SSTableReader> sstables, final Collection<Range> ranges)
                throws IOException
        {
            org.apache.commons.collections.Predicate rangesPredicate = new org.apache.commons.collections.Predicate()
            {
                public boolean evaluate(Object row)
                {
                    return Range.isTokenInRanges(((SSTableIdentityIterator)row).getKey().token, ranges);
                }
            };
            CollatingIterator iter = FBUtilities.<SSTableIdentityIterator>getCollatingIterator();
            for (SSTableReader sstable : sstables)
            {
                SSTableScanner scanner = sstable.getScanner(FILE_BUFFER_SIZE);
                iter.addIterator(new FilterIterator(scanner, rangesPredicate));
            }
            return iter;
        }

        public Iterable<SSTableScanner> getScanners()
        {
            if (scanners == null)
            {
                scanners = new HashSet<SSTableScanner>();
                for (Object o : ((CollatingIterator)source).getIterators())
                {
                    scanners.add((SSTableScanner)((FilterIterator)o).getIterator());
                }
            }
            return scanners;
        }
    }

//TODO: TEST (AES support for Counter CFs)
    private static class CounterAESCompactionIterator extends AntiCompactionIterator
    {
        private InetAddress remoteAddress;

        public CounterAESCompactionIterator(Collection<SSTableReader> sstables, Collection<Range> ranges, int gcBefore, boolean isMajor, InetAddress remoteAddress)
                throws IOException
        {
            super(sstables, ranges, gcBefore, isMajor);

            this.remoteAddress = remoteAddress;
        }
        
        @Override
        protected ColumnFamily calculatePurgedColumnFamily(ColumnFamily cf)
        {
            cf = super.calculatePurgedColumnFamily(cf);
            cf.cleanContext(remoteAddress);
            // same check as ColumnFamilyStore.removeDeleted()
            if (cf.getColumnCount() == 0 && cf.getLocalDeletionTime() <= gcBefore)
                return null;

            return cf;
        }
    }

    public void checkAllColumnFamilies() throws IOException
    {
        // perform estimates
        for (final ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            Runnable runnable = new Runnable()
            {
                public void run ()
                {
                    logger.debug("Estimating compactions for " + cfs.columnFamily_);
                    final Set<List<SSTableReader>> buckets = getBuckets(cfs.getSSTables(), 50L * 1024L * 1024L);
                    updateEstimateFor(cfs, buckets);
                }
            };
            executor.submit(runnable);
        }

        // actually schedule compactions.  done in a second pass so all the estimates occur before we
        // bog down the executor in actual compactions.
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            submitMinorIfNeeded(cfs);
        }
    }

    private static class CompactionExecutor extends DebuggableThreadPoolExecutor
    {
        private volatile ColumnFamilyStore cfs;
        private volatile CompactionIterator ci;

        public CompactionExecutor()
        {
            super("COMPACTION-POOL");
        }

        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            cfs = null;
            ci = null;
        }

        void beginCompaction(ColumnFamilyStore cfs, CompactionIterator ci)
        {
            this.cfs = cfs;
            this.ci = ci;
        }

        public String getColumnFamilyName()
        {
            return cfs == null ? null : cfs.getColumnFamilyName();
        }

        public Long getBytesTotal()
        {
            return ci == null ? null : ci.getTotalBytes();
        }

        public Long getBytesCompleted()
        {
            return ci == null ? null : ci.getBytesRead();
        }
    }

    public String getColumnFamilyInProgress()
    {
        return executor.getColumnFamilyName();
    }

    public Long getBytesTotalInProgress()
    {
        return executor.getBytesTotal();
    }

    public Long getBytesCompacted()
    {
        return executor.getBytesCompleted();
    }

    public int getPendingTasks()
    {
        int n = 0;
        for (Integer i : estimatedCompactions.values())
        {
            n += i;
        }
        return n;
    }
}
