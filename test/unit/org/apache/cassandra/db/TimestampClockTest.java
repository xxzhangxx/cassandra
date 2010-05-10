/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;

public class TimestampClockTest
{   
    @Test
    public void testCompare()
    {
        TimestampClock clock;
        TimestampClock other;

        // greater than
        clock = new TimestampClock(3);
        other = new TimestampClock(1);
        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new TimestampClock(2);
        other = new TimestampClock(2);
        assert clock.compare(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new TimestampClock(1);
        other = new TimestampClock(3);
        assert clock.compare(other) == IClock.ClockRelationship.LESS_THAN;
    }

    @Test
    public void testGetSuperset()
    {
        // empty list
        assert ((TimestampClock)TimestampClock.MIN_VALUE.getSuperset(new LinkedList<IClock>())).timestamp() == TimestampClock.MIN_VALUE.timestamp();

        // normal list
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(new TimestampClock(0));
        clocks.add(new TimestampClock(20300));
        clocks.add(new TimestampClock(1));
        clocks.add(new TimestampClock(3));
        clocks.add(new TimestampClock(2839));

        assert ((TimestampClock)TimestampClock.MIN_VALUE.getSuperset(clocks)).timestamp() == 20300;
    }

    @Test
    public void testSerializeDeserialize() throws IOException
    {
        TimestampClock clock = new TimestampClock(3280982);

        // size
        DataOutputBuffer bufOut = new DataOutputBuffer();
        TimestampClock.SERIALIZER.serialize(clock, bufOut);

        assert bufOut.getLength() == clock.size();

        // equality
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        TimestampClock deserialized = (TimestampClock)TimestampClock.SERIALIZER.deserialize(new DataInputStream(bufIn));

        assert clock.timestamp() == deserialized.timestamp();
    }
}
