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

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.context.IncrementCounterContext;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class IncrementCounterClock implements IClock
{
    public static IncrementCounterClock MIN_VALUE = new IncrementCounterClock(FBUtilities.toByteArray(Long.MIN_VALUE));
    public static ICompactSerializer2<IClock> SERIALIZER = new IncrementCounterClockSerializer();

    private static IncrementCounterContext contextManager = IncrementCounterContext.instance();

    public byte[] context;

    public IncrementCounterClock()
    {
        this.context = contextManager.create();
    }

    public IncrementCounterClock(byte[] context)
    {
        this.context = context;
    }

    public byte[] context()
    {
        return context;
    }

    public void update(InetAddress node, long delta)
    {
        context = contextManager.update(context, node, delta);
    }

    public ClockRelationship compare(IClock other)
    {
        assert other instanceof IncrementCounterClock : "Wrong class type.";

        ContextRelationship rel = contextManager.compare(context, ((IncrementCounterClock)other).context());
        return convertClockRelationship(rel);
    }

    public ClockRelationship diff(IClock other)
    {
        assert other instanceof IncrementCounterClock : "Wrong class type.";

        ContextRelationship rel = contextManager.diff(context, ((IncrementCounterClock)other).context());
        return convertClockRelationship(rel);
    }
    
    //TODO can't we reduce the clock relationships into one?
    private ClockRelationship convertClockRelationship(ContextRelationship rel)
    {
        switch (rel)
        {
            case EQUAL:
                return ClockRelationship.EQUAL;
            case GREATER_THAN:
                return ClockRelationship.GREATER_THAN;
            case LESS_THAN:
                return ClockRelationship.LESS_THAN;
            default: // DISJOINT
                return ClockRelationship.DISJOINT;
        }
    }
    
    @Override
    public IColumn diff(IColumn left, IColumn right)
    {
        // data encapsulated in clock
        if (ClockRelationship.GREATER_THAN == ((IncrementCounterClock)left.clock()).diff(right.clock()))
        {
            return left;
        }
        return null;
    }

    public IClock getSuperset(List<IClock> otherClocks)
    {
        List<byte[]> contexts = new LinkedList<byte[]>();

        contexts.add(context);
        for (IClock clock : otherClocks)
        {
            assert clock instanceof IncrementCounterClock : "Wrong class type.";
            contexts.add(((IncrementCounterClock)clock).context);
        }

        return new IncrementCounterClock(contextManager.merge(contexts));
    }

    public int size()
    {
        return DBConstants.intSize_ + context.length;
    }

    public void serialize(DataOutput out) throws IOException
    {
        SERIALIZER.serialize(this, out);
    }

    public String toString()
    {
        return contextManager.toString(context);
    }

    public void cleanNodeCounts(InetAddress node)
    {
        context = contextManager.cleanNodeCounts(context, node);
    }

    @Override
    public ClockType type()
    {
        return ClockType.IncrementCounter;
    }

}

class IncrementCounterClockSerializer implements ICompactSerializer2<IClock> 
{
    public void serialize(IClock c, DataOutput out) throws IOException
    {
        FBUtilities.writeByteArray(((IncrementCounterClock)c).context(), out);
    }

    public IClock deserialize(DataInput in) throws IOException
    {
        int length = in.readInt();
        if ( length < 0 )
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }
        byte[] context = new byte[length];
        if ( length > 0 )
        {
            in.readFully(context);
        }
        return new IncrementCounterClock(context);
    }
}
