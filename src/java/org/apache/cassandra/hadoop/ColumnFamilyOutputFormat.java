package org.apache.cassandra.hadoop;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

/**
 * The <code>ColumnFamilyOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * values) as Cassandra rows (and respective columns) in a given
 * {@link ColumnFamily}.
 * 
 * <p>
 * As is the case with the {@link ColumnFamilyInputFormat}, you need to set the
 * CF and predicate (description of columns to extract from each row) in your
 * Hadoop job Configuration. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setColumnFamily} and
 * {@link ConfigHelper#setSlicePredicate} methods, is provided to make this
 * simple.
 * </p>
 * 
 * <p>
 * By default, it prevents overwriting existing rows in the column family, by
 * ensuring at initialization time that it contains no rows in the given slice
 * predicate. For the sake of performance, it employs a lazy write-back caching
 * mechanism, where its record writer batches mutations created based on the
 * reduce's inputs (in a task-specific map). When the writer is closed, then it
 * makes the changes official by sending a batch mutate request to Cassandra.
 * </p>
 * 
 * @author Karthick Sankarachary
 */
public class ColumnFamilyOutputFormat extends OutputFormat<byte[],List<IColumn>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyOutputFormat.class);
    
    public static final String BATCH_THRESHOLD = "mapreduce.output.columnfamilyoutputformat.batch.threshold";
    
    /**
     * Check for validity of the output-specification for the job.
     * 
     * <p>
     * This is to validate the output specification for the job when it is a job
     * is submitted. By default, it will prevent writes to the given column
     * family, if it already contains one or more rows in the given slice
     * predicate. If you wish to relax that restriction, you may override this
     * method is a sub-class of your choosing.
     * </p>
     * 
     * @param context
     *            information about the job
     * @throws IOException
     *             when output should not be attempted
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
    {
        validateConfiguration(context.getConfiguration());
        String keyspace = ConfigHelper.getKeyspace(context.getConfiguration());
        String columnFamily = ConfigHelper.getColumnFamily(context.getConfiguration());
        SlicePredicate slicePredicate = ConfigHelper.getSlicePredicate(context.getConfiguration());
        assert slicePredicate != null;
        if (slicePredicate.column_names == null && slicePredicate.slice_range == null)
            slicePredicate = slicePredicate.setColumn_names(new ArrayList<byte[]>());

        List<KeySlice> keySlices;
        try
        {
            TSocket socket = new TSocket(DatabaseDescriptor.getListenAddress().getHostName(), DatabaseDescriptor.getRpcPort());
            Cassandra.Client client = createAuthenticatedClient(socket, context);
            ColumnParent parent = new ColumnParent().setColumn_family(columnFamily);
            KeyRange range = new KeyRange().setStart_key("".getBytes()).setEnd_key("".getBytes());
            keySlices = client.get_range_slices(parent, slicePredicate, range, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        if (keySlices.size() > 0)
        {
            throw new IOException("The column family " + columnFamily
                                  + " in the keyspace " + keyspace + " already has "
                                  + keySlices.size() + " keys in the slice predicate "
                                  + slicePredicate);
        }
    }
    
    /**
     * Get the output committer for this output format. This is responsible for
     * ensuring the output is committed correctly.
     * 
     * <p>
     * This output format employs a lazy write-back caching mechanism, where the
     * {@link RecordWriter} is responsible for collecting mutations in the
     * {@link #MUTATIONS_CACHE}, and the {@link OutputCommitter} makes the
     * changes official by making the change request to Cassandra.
     * </p>
     * 
     * @param context
     *            the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }
    
    /**
     * Get the {@link RecordWriter} for the given task.
     * 
     * <p>
     * As stated above, this {@link RecordWriter} merely batches the mutations
     * that it defines in the {@link #MUTATIONS_CACHE}. In other words, it
     * doesn't literally cause any changes on the Cassandra server.
     * </p>
     * 
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<byte[],List<IColumn>> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new ColumnFamilyRecordWriter(context);
    }
    
    /**
     * Ensure that this output format has been configured correctly, with a
     * valid keyspace, column family and slice predicate.
     * 
     * @param conf
     */
    public void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getKeyspace(conf) == null || ConfigHelper.getColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (ConfigHelper.getSlicePredicate(conf) == null)
        {
            System.out.println("Since no slice predicate was specified, all columns in "
                               + ConfigHelper.getColumnFamily(conf)
                               + " will be overwritten");
        }
    }

    /**
     * Return a client based on the given socket that points to the configured
     * keyspace, and is logged in with the configured credentials.
     *
     * @param socket  a socket pointing to a particular node, seed or otherwise
     * @param context a job context
     * @return a cassandra client
     * @throws InvalidRequestException
     * @throws TException
     * @throws AuthenticationException
     * @throws AuthorizationException
     */
    public static Cassandra.Client createAuthenticatedClient(TSocket socket, JobContext context)
    throws InvalidRequestException, TException, AuthenticationException, AuthorizationException
    {
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        socket.open();
        client.set_keyspace(ConfigHelper.getKeyspace(context.getConfiguration()));
        Map<String, String> creds = new HashMap<String, String>();
        creds.put(SimpleAuthenticator.USERNAME_KEY, ConfigHelper.getKeyspaceUserName(context.getConfiguration()));
        creds.put(SimpleAuthenticator.PASSWORD_KEY, ConfigHelper.getKeyspacePassword(context.getConfiguration()));
        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
        if (!(DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator))
            client.login(authRequest);
        return client;

    }

    /**
     * An {@link OutputCommitter} that does nothing.
     */
    public class NullOutputCommitter extends OutputCommitter
    {
        public void abortTask(TaskAttemptContext taskContext) { }

        public void cleanupJob(JobContext jobContext) { }

        public void commitTask(TaskAttemptContext taskContext) { }

        public boolean needsTaskCommit(TaskAttemptContext taskContext)
        {
            return false;
        }

        public void setupJob(JobContext jobContext) { }

        public void setupTask(TaskAttemptContext taskContext) { }
    }
}
