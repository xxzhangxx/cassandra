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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.clock.VersionVectorContext;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;

public class VersionVectorClockTest
{
    private static final VersionVectorContext vvc = new VersionVectorContext();

    private static final int idLength;
    private static final int countLength;
    private static final int stepLength;

    static
    {
        idLength        = 4; // size of int
        countLength     = 8; // size of long
        stepLength      = idLength + countLength;
    }

    @Test
    public void testUpdate() throws UnknownHostException
    {
        VersionVectorClock clock;

        // note: updates are in-place
        clock = new VersionVectorClock(new byte[0]);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));
        
        assert clock.context().length == stepLength;

        assert 1  == FBUtilities.byteArrayToInt(clock.context(),  0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(clock.context(), 0*stepLength + idLength);

        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));

        assert clock.context().length == 2 * stepLength;

        assert 2  == FBUtilities.byteArrayToInt(clock.context(),  0*stepLength);
        assert 3L == FBUtilities.byteArrayToLong(clock.context(), 0*stepLength + idLength);

        assert 1  == FBUtilities.byteArrayToInt(clock.context(),  1*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(clock.context(), 1*stepLength + idLength);
    }

    @Test
    public void testCompare() throws UnknownHostException
    {
        VersionVectorClock clock;
        VersionVectorClock other;

        // greater than
        clock = new VersionVectorClock(new byte[0]);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));

        other = new VersionVectorClock(new byte[0]);

        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new VersionVectorClock(new byte[0]);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));

        other = new VersionVectorClock(new byte[0]);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));

        assert clock.compare(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new VersionVectorClock(new byte[0]);

        other = new VersionVectorClock(new byte[0]);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));

        assert clock.compare(other) == IClock.ClockRelationship.LESS_THAN;

        // disjoint
        clock = new VersionVectorClock(new byte[0]);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)));

        other = new VersionVectorClock(new byte[0]);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(9)));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)));

        assert clock.compare(other) == IClock.ClockRelationship.DISJOINT;
    }

    @Test
    public void testGetSuperset()
    {
        // empty list
        assert ((VersionVectorClock)VersionVectorClock.MIN_VALUE.getSuperset(new LinkedList<IClock>())).context().length == 0;

        // normal list
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            )));
        clocks.add(new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L)
            )));
        clocks.add(new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(14L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(13L)
            )));
        clocks.add(new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(999L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(632L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(45L)
            )));
        clocks.add(new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(1234L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(655L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L)
            )));

        // 5:   1234L
        // 2:   999L
        // 3:   655L
        // 4:   632L
        // 1:   128L
        // 9:   62L
        // 8:   45L
        // 6:   2L
        // 7:   1L

        byte[] merged = ((VersionVectorClock)VersionVectorClock.MIN_VALUE.getSuperset(clocks)).context();

        assert 5 == FBUtilities.byteArrayToInt(merged, 0*stepLength);
        assert 1234L == FBUtilities.byteArrayToLong(merged, 0*stepLength + idLength);

        assert 2 == FBUtilities.byteArrayToInt(merged, 1*stepLength);
        assert 999L == FBUtilities.byteArrayToLong(merged, 1*stepLength + idLength);

        assert 3 == FBUtilities.byteArrayToInt(merged, 2*stepLength);
        assert 655L == FBUtilities.byteArrayToLong(merged, 2*stepLength + idLength);

        assert 4 == FBUtilities.byteArrayToInt(merged, 3*stepLength);
        assert 632L == FBUtilities.byteArrayToLong(merged, 3*stepLength + idLength);

        assert 1 == FBUtilities.byteArrayToInt(merged, 4*stepLength);
        assert 128L == FBUtilities.byteArrayToLong(merged, 4*stepLength + idLength);

        assert 9 == FBUtilities.byteArrayToInt(merged, 5*stepLength);
        assert 62L == FBUtilities.byteArrayToLong(merged, 5*stepLength + idLength);

        assert 8 == FBUtilities.byteArrayToInt(merged, 6*stepLength);
        assert 45L == FBUtilities.byteArrayToLong(merged, 6*stepLength + idLength);

        assert 6 == FBUtilities.byteArrayToInt(merged, 7*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, 7*stepLength + idLength);

        assert 7 == FBUtilities.byteArrayToInt(merged, 8*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, 8*stepLength + idLength);
    }

    @Test
    public void testSerializeDeserialize() throws IOException, UnknownHostException
    {
        VersionVectorClock clock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(912L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(35L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L)
            ));

        // size
        DataOutputBuffer bufOut = new DataOutputBuffer();
        VersionVectorClock.SERIALIZER.serialize(clock, bufOut);

        assert bufOut.getLength() == clock.size();

        // equality
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        VersionVectorClock deserialized = (VersionVectorClock)VersionVectorClock.SERIALIZER.deserialize(new DataInputStream(bufIn));

        assert 0 == FBUtilities.compareByteArrays(clock.context(), deserialized.context());
    }
}
