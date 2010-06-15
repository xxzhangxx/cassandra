package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class SSTableReaderTest extends CleanupHelper
{
    @Test
    public void testSpannedIndexPositions() throws IOException, ExecutionException, InterruptedException
    {
        MmappedSegmentedFile.MAX_SEGMENT_SIZE = 40; // each index entry is ~11 bytes, so this will generate lots of segments

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 100; j += 2)
        {
            byte[] key = String.valueOf(j).getBytes();
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, "0".getBytes()), new byte[0], new TimestampClock(j));
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.submitMajor(store).get();

        // check that all our keys are found correctly
        SSTableReader sstable = store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = sstable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            assert sstable.getPosition(dk) == -1;
        }
    }
}
