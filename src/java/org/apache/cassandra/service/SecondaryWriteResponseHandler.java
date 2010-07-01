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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles blocking secondary writes for ONE, ANY, QUORUM, and ALL consistency levels.
 */
public class SecondaryWriteResponseHandler extends WriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(SecondaryWriteResponseHandler.class);

    protected AtomicInteger responses;

    public SecondaryWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, hintedEndpoints, consistencyLevel, table);
    }

    @Override
    protected int determineBlockFor(String table)
    {
        int blockFor = super.determineBlockFor(table) - 1;
        return Math.max(0, blockFor);
    }
}
