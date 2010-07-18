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

import org.apache.cassandra.db.clock.VersionVectorContext;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class VersionVectorClock implements IClock
{
    public static ICompactSerializer2<IClock> SERIALIZER = new VersionVectorClockSerializer();

    private static VersionVectorContext contextManager = VersionVectorContext.instance();

    public static VersionVectorClock MIN_VALUE = new VersionVectorClock(contextManager.create());

    public byte[] context;

    public VersionVectorClock()
    {
        this.context = contextManager.create();
    }

    public VersionVectorClock(byte[] context)
    {
        this.context = context;
    }

    public byte[] context()
    {
        return context;
    }

    public void update(InetAddress node)
    {
        context = contextManager.update(context, node);
    }

    public ClockRelationship compare(IClock other)
    {
        assert other instanceof VersionVectorClock : "Wrong class type.";

        return contextManager.compare(context, ((VersionVectorClock)other).context());
    }

    public IColumn diff(IColumn left, IColumn right)
    {
        // note: DISJOINT is not relevant
        //   column will be created by resolve(), which reconciles disjoint versions
        if (ClockRelationship.GREATER_THAN == left.clock().compare(right.clock()))
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
            assert clock instanceof VersionVectorClock : "Wrong class type.";
            contexts.add(((VersionVectorClock)clock).context);
        }

        return new VersionVectorClock(contextManager.merge(contexts));
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

    public ClockType type()
    {
        return ClockType.VersionVector;
    }

    public void update(ColumnFamily cf, InetAddress node)
    {
        // standard column family
        if (!cf.isSuper())
        {
            for (IColumn col : cf.getSortedColumns())
            {
                // update in-place, although Column is (abstractly) immutable
                ((VersionVectorClock)col.clock()).update(node);
            }
            return;
        }

        // super column family
        for (IColumn col : cf.getSortedColumns())
        {
            for (IColumn subCol : col.getSubColumns())
            {
                // update in-place, although Column is (abstractly) immutable
                ((VersionVectorClock)subCol.clock()).update(node);
            }
        }
    }
}

class VersionVectorClockSerializer implements ICompactSerializer2<IClock> 
{
    public void serialize(IClock c, DataOutput out) throws IOException
    {
        FBUtilities.writeByteArray(((VersionVectorClock)c).context(), out);
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
        return new VersionVectorClock(context);
    }
}
