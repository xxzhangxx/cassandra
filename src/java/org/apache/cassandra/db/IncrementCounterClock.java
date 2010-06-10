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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.clock.AbstractCounterContext;
import org.apache.cassandra.db.clock.IncrementCounterContext;
import org.apache.cassandra.db.clock.IContext.ContextRelationship;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class IncrementCounterClock extends AbstractCounterClock
{
    private static Logger logger = Logger.getLogger(IncrementCounterClock.class);

    public static IncrementCounterClock MIN_VALUE = new IncrementCounterClock(
        FBUtilities.toByteArray(Long.MIN_VALUE));
    public static ICompactSerializer2<IClock> SERIALIZER = new IncrementCounterClockSerializer();

    private static IncrementCounterContext contextManager = IncrementCounterContext.instance();

    public IncrementCounterClock()
    {
        super();
    }

    public IncrementCounterClock(byte[] context)
    {
        super(context);
    }

    public AbstractCounterContext getContextManager()
    {
        return contextManager;
    }

    protected AbstractCounterClock createClock(byte[] context)
    {
        return new IncrementCounterClock(context);
    }

    public ICompactSerializer2<IClock> getSerializer()
    {
        return SERIALIZER;
    }

    @Override
    public ClockType type()
    {
        return ClockType.IncrementCounter;
    }
}

class IncrementCounterClockSerializer extends AbstractCounterClockSerializer
{
    protected AbstractCounterClock createClock(byte[] context)
    {
        return new IncrementCounterClock(context);
    }
}
