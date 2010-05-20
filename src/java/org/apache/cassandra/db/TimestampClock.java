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
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer2;

public class TimestampClock implements IClock
{
    public static TimestampClock MIN_VALUE = new TimestampClock(Long.MIN_VALUE);
    public static TimestampClock ZERO = new TimestampClock(0L);
    public static ICompactSerializer2<IClock> SERIALIZER = new TimestampClockSerializer();

    private final long timestamp;

    public TimestampClock(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public ClockRelationship compare(IClock other)
    {
        assert other instanceof TimestampClock : "Wrong class type.";

        int compare = (new Long(timestamp)).compareTo(
            new Long(((TimestampClock)other).timestamp()));
        if (compare > 0)
        {
            return ClockRelationship.GREATER_THAN;
        }
        else if (compare == 0)
        {
            return ClockRelationship.EQUAL;
        }
        // compare < 0
        return ClockRelationship.LESS_THAN;
    }

    public IClock getSuperset(List<IClock> otherClocks)
    {
        IClock max = this;

        for (IClock clock : otherClocks)
        {
            if (clock.compare(max) == ClockRelationship.GREATER_THAN)
            {
                max = clock;
            }
        }

        return max;
    }

    public int size()
    {
        return DBConstants.tsSize_;
    }

    public void serialize(DataOutput out) throws IOException
    {
        SERIALIZER.serialize(this, out);
    }

    public String toString()
    {
        return Long.toString(timestamp);
    }
}

class TimestampClockSerializer implements ICompactSerializer2<IClock> 
{
    public void serialize(IClock tc, DataOutput out) throws IOException
    {
        out.writeLong(((TimestampClock)tc).timestamp());
    }

    public IClock deserialize(DataInput in) throws IOException
    {
        return new TimestampClock(in.readLong());
    }
}
