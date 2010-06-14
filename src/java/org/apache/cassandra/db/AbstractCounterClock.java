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
import org.apache.cassandra.db.clock.IContext.ContextRelationship;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractCounterClock implements IClock
{
    private static Logger logger_ = Logger.getLogger(AbstractCounterClock.class);

    public byte[] context;

    public AbstractCounterClock()
    {
        this.context = getContextManager().create();
    }

    public AbstractCounterClock(byte[] context)
    {
        this.context = context;
    }

    public abstract AbstractCounterContext getContextManager();
    protected abstract AbstractCounterClock createClock(byte[] context);

    public abstract ICompactSerializer2<IClock> getSerializer();

    public byte[] context()
    {
        return context;
    }

    public void update(InetAddress node, long delta)
    {
        context = getContextManager().update(context, node, delta);
    }

    public ClockRelationship compare(IClock other)
    {
        assert other instanceof AbstractCounterClock : "Wrong class type.";

        ContextRelationship rel = getContextManager().compare(context, ((AbstractCounterClock)other).context());
        return convertClockRelationship(rel);
    }

    public ClockRelationship diff(IClock other)
    {
        assert other instanceof AbstractCounterClock : "Wrong class type.";

        ContextRelationship rel = getContextManager().diff(context, ((AbstractCounterClock)other).context());
        return convertClockRelationship(rel);
    }

    //TODO can't we reduce the clock relationships into one?
    private ClockRelationship convertClockRelationship(ContextRelationship rel)
    {
        switch (rel) {
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
        if (ClockRelationship.GREATER_THAN == ((AbstractCounterClock)left.clock()).diff(right.clock())) {
            return left;
        }
        return null;
    }

    public int size()
    {
        return DBConstants.intSize_ + context.length;
    }

    public void serialize(DataOutput out) throws IOException
    {
        getSerializer().serialize(this, out);
    }

    public String toString()
    {
        return getContextManager().toString(context);
    }

    public void cleanNodeCounts(InetAddress node)
    {
        context = getContextManager().cleanNodeCounts(context, node);
    }

    @Override
    public void cleanContext(IColumnContainer cc, InetAddress node)
    {
        //TODO: REFACTOR? (modify: 1) where CF is sanitized to be on read side; 2) how CF is sanitized)
        //TODO: TEST (clean remote replica counts for read repair)
        
        for (IColumn column : cc.getSortedColumns())
        {
            if (column instanceof SuperColumn) {
                cleanContext((IColumnContainer)column, node);
                continue;
            }

            AbstractCounterClock clock = (AbstractCounterClock)column.clock();
            clock.cleanNodeCounts(node);
            if (0 == clock.context().length)
                cc.remove(column.name());
        }
    }

    @Override
    public void update(ColumnFamily cf, InetAddress node)
    {
        // standard column family
        if (!cf.isSuper()) {
            for (IColumn col : cf.getSortedColumns())
            {
                if (col.isMarkedForDelete())
                    continue;

                // TODO: MODIFY: prob need to create new Column()
                // update in-place, although Column is (abstractly) immutable
                ((AbstractCounterClock) col.clock()).update(node, FBUtilities.byteArrayToLong(col.value()));

            }
            return;
        }

        // super column family
        for (IColumn col : cf.getSortedColumns())
        {
            for (IColumn subCol : col.getSubColumns())
            {
                if (subCol.isMarkedForDelete())
                    continue;

                // TODO: MODIFY: prob need to create new Column()
                ((AbstractCounterClock) subCol.clock()).update(node, FBUtilities.byteArrayToLong(subCol.value()));
            }
        }
    }

    public IClock getSuperset(List<IClock> otherClocks)
    {
        List<byte[]> contexts = new LinkedList<byte[]>();

        contexts.add(context);
        for (IClock clock : otherClocks)
        {
            assert clock instanceof AbstractCounterClock : "Wrong class type.";
            contexts.add(((AbstractCounterClock)clock).context);
        }

        return createClock(getContextManager().merge(contexts));
    }
}

abstract class AbstractCounterClockSerializer implements ICompactSerializer2<IClock> 
{
    protected abstract AbstractCounterClock createClock(byte[] context);

    public void serialize(IClock c, DataOutput out) throws IOException
    {
        FBUtilities.writeByteArray(((AbstractCounterClock)c).context(), out);
    }

    public IClock deserialize(DataInput in) throws IOException
    {
        int length = in.readInt();
        if ( length < 0 )
            throw new IOException("Corrupt (negative) value length encountered");

        byte[] context = new byte[length];
        if ( length > 0 )
            in.readFully(context);

        return createClock(context);
    }
}
