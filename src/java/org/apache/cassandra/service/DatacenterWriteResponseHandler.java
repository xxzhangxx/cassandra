/**
 *
 */
package org.apache.cassandra.service;
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


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractRackAwareSnitch;
import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Multimap;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.DCQUORUM).
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private static final AbstractRackAwareSnitch snitch = (AbstractRackAwareSnitch) DatabaseDescriptor.getEndpointSnitch();

    private static final String localdc;
    static
    {
        localdc = snitch.getDatacenter(FBUtilities.getLocalAddress());
    }

    public DatacenterWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, hintedEndpoints, consistencyLevel, table);
        assert consistencyLevel == ConsistencyLevel.DCQUORUM;
    }


    @Override
    protected int determineBlockFor(String table)
    {
        DatacenterShardStrategy strategy = (DatacenterShardStrategy) StorageService.instance.getReplicationStrategy(table);
        return (strategy.getReplicationFactor(localdc) / 2) + 1;
    }


    @Override
    public void response(Message message)
    {
        if (message == null || localdc.equals(snitch.getDatacenter(message.getFrom())))
        {
            if (responses.decrementAndGet() == 0)
                condition.signal();
        }
    }
    
    @Override
    public void assureSufficientLiveNodes() throws UnavailableException
    {
        int liveNodes = 0;
        for (InetAddress destination : writeEndpoints)
        {
            if (localdc.equals(snitch.getDatacenter(destination)))
                liveNodes++;
        }

        if (liveNodes < responses.get())
        {
            throw new UnavailableException();
        }
    }
}
