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

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.db.clock.StandardCounterContext;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;

public class StandardCounterClockTest
{
    private static final int timestampLength;

    private static final int idLength;
    private static final int incrCountLength;
    private static final int decrCountLength;

    private static final int stepLength;

    static
    {
        timestampLength = 8; // size of long

        idLength        = 4; // size of int
        incrCountLength = 8; // size of long
        decrCountLength = 8; // size of long

        stepLength      = idLength + incrCountLength + decrCountLength;
    }

    @Test
    public void testUpdate() throws UnknownHostException
    {
        StandardCounterClock clock;

        // note: updates are in-place
        clock = new StandardCounterClock();
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.context().length == (timestampLength + stepLength);

        assert  1 == FBUtilities.byteArrayToInt( clock.context(), timestampLength + 0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(clock.context(), timestampLength + 0*stepLength + idLength);

        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)),  3L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), -2L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)),  9L);

        assert clock.context().length == (timestampLength + 2 * stepLength);

        assert   2 == FBUtilities.byteArrayToInt(clock.context(),  timestampLength + 0*stepLength);
        assert 12L == FBUtilities.byteArrayToLong(clock.context(), timestampLength + 0*stepLength + idLength);

        assert   1 == FBUtilities.byteArrayToInt(clock.context(),  timestampLength + 1*stepLength);
        assert  1L == FBUtilities.byteArrayToLong(clock.context(), timestampLength + 1*stepLength + idLength);
    }

    @Test
    public void testCompare() throws InterruptedException, UnknownHostException
    {
        StandardCounterClock clock;
        StandardCounterClock other;

        // greater than
        clock = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(10L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            ));
        other = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(3L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        other = new StandardCounterClock();
        Thread.sleep(10);

        clock = new StandardCounterClock();
        clock.update(FBUtilities.getLocalAddress(),   1L);
        clock.update(FBUtilities.getLocalAddress(), -10L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(3)), 1L);

        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        other = new StandardCounterClock();
        other.update(FBUtilities.getLocalAddress(), 1L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(3)), -9L);
        Thread.sleep(10);

        clock = new StandardCounterClock();
        clock.update(FBUtilities.getLocalAddress(),   1L);
        clock.update(FBUtilities.getLocalAddress(), -10L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(3)), 1L);

        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            ));
        other = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));
        other = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.LESS_THAN;

        // disjoint: not possible
    }

    @Test
    public void testDiff() throws UnknownHostException
    {
        StandardCounterClock clock;
        StandardCounterClock other;

        // greater than
        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));

        assert clock.diff(other) == IClock.ClockRelationship.GREATER_THAN;

        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), -1L);

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));

        assert clock.diff(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.EQUAL;

        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), -2L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), -2L);

        assert clock.diff(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.LESS_THAN;

        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), -1L);

        assert clock.diff(other) == IClock.ClockRelationship.LESS_THAN;

        // disjoint
        clock = new StandardCounterClock(FBUtilities.toByteArray(0L));
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)),  1L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)),  1L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), -1L);

        other = new StandardCounterClock(FBUtilities.toByteArray(0L));
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(9)), 1L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.DISJOINT;
    }

    @Test
    public void testGetSuperset()
    {
        // empty list
        assert ((StandardCounterClock)StandardCounterClock.MIN_VALUE.getSuperset(new LinkedList<IClock>())).context().length == timestampLength;

        // normal list
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L), FBUtilities.toByteArray(18L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),  FBUtilities.toByteArray(21L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(13L)
            )));
        clocks.add(new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(4L),  FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L),  FBUtilities.toByteArray(9L)
            )));
        clocks.add(new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(15L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(14L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(13L), FBUtilities.toByteArray(2L)
            )));
        clocks.add(new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(12L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(999L), FBUtilities.toByteArray(100L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(632L), FBUtilities.toByteArray(321L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(45L),  FBUtilities.toByteArray(54L)
            )));
        clocks.add(new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(15L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1234L), FBUtilities.toByteArray(974L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(655L), FBUtilities.toByteArray(242L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L),   FBUtilities.toByteArray(0L)
            )));

        // node:        incr:    decr:  total:
        // 127.0.0.1:   1266L     987L   2253L
        // 2:            999L     100L   1099L
        // 4:            632L     321L    953L
        // 3:            655L     242L    897L
        // 1:            128L      18L    146L
        // 8:             45L      54L     99L
        // 9:             62L      21L     83L
        // 6:              2L       9L     11L
        // 7:              1L       0L      1L

        byte[] merged = ((StandardCounterClock)StandardCounterClock.MIN_VALUE.getSuperset(clocks)).context();

        assert 0 == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged,
            timestampLength + 0*stepLength,
            4);
        assert 1266L == FBUtilities.byteArrayToLong(merged, timestampLength + 0*stepLength + idLength);
        assert  987L == FBUtilities.byteArrayToLong(merged, timestampLength + 0*stepLength + idLength + incrCountLength);

        assert    2 == FBUtilities.byteArrayToInt(merged,  timestampLength + 1*stepLength);
        assert 999L == FBUtilities.byteArrayToLong(merged, timestampLength + 1*stepLength + idLength);
        assert 100L == FBUtilities.byteArrayToLong(merged, timestampLength + 1*stepLength + idLength + incrCountLength);

        assert    4 == FBUtilities.byteArrayToInt(merged,  timestampLength + 2*stepLength);
        assert 632L == FBUtilities.byteArrayToLong(merged, timestampLength + 2*stepLength + idLength);
        assert 321L == FBUtilities.byteArrayToLong(merged, timestampLength + 2*stepLength + idLength + incrCountLength);

        assert    3 == FBUtilities.byteArrayToInt(merged,  timestampLength + 3*stepLength);
        assert 655L == FBUtilities.byteArrayToLong(merged, timestampLength + 3*stepLength + idLength);
        assert 242L == FBUtilities.byteArrayToLong(merged, timestampLength + 3*stepLength + idLength + incrCountLength);

        assert    1 == FBUtilities.byteArrayToInt(merged,  timestampLength + 4*stepLength);
        assert 128L == FBUtilities.byteArrayToLong(merged, timestampLength + 4*stepLength + idLength);
        assert  18L == FBUtilities.byteArrayToLong(merged, timestampLength + 4*stepLength + idLength + incrCountLength);

        assert   8 == FBUtilities.byteArrayToInt(merged,  timestampLength + 5*stepLength);
        assert 45L == FBUtilities.byteArrayToLong(merged, timestampLength + 5*stepLength + idLength);
        assert 54L == FBUtilities.byteArrayToLong(merged, timestampLength + 5*stepLength + idLength + incrCountLength);

        assert   9 == FBUtilities.byteArrayToInt(merged,  timestampLength + 6*stepLength);
        assert 62L == FBUtilities.byteArrayToLong(merged, timestampLength + 6*stepLength + idLength);
        assert 21L == FBUtilities.byteArrayToLong(merged, timestampLength + 6*stepLength + idLength + incrCountLength);

        assert   6 == FBUtilities.byteArrayToInt(merged,  timestampLength + 7*stepLength);
        assert  2L == FBUtilities.byteArrayToLong(merged, timestampLength + 7*stepLength + idLength);
        assert  9L == FBUtilities.byteArrayToLong(merged, timestampLength + 7*stepLength + idLength + incrCountLength);

        assert   7 == FBUtilities.byteArrayToInt(merged,  timestampLength + 8*stepLength);
        assert  1L == FBUtilities.byteArrayToLong(merged, timestampLength + 8*stepLength + idLength);
        assert  0L == FBUtilities.byteArrayToLong(merged, timestampLength + 8*stepLength + idLength + incrCountLength);
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        StandardCounterClock clock = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(912L), FBUtilities.toByteArray(132L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(35L),  FBUtilities.toByteArray(23L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(15L),  FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(6L),   FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L),   FBUtilities.toByteArray(0L)
            ));
        byte[] bytes = clock.context();

        assert   9 == FBUtilities.byteArrayToInt(bytes,  timestampLength + 3*stepLength);
        assert  6L == FBUtilities.byteArrayToLong(bytes, timestampLength + 3*stepLength + idLength);
        assert  4L == FBUtilities.byteArrayToLong(bytes, timestampLength + 3*stepLength + idLength + incrCountLength);

        clock.cleanNodeCounts(InetAddress.getByAddress(FBUtilities.toByteArray(9)));
        bytes = clock.context();

        // node: 0.0.0.9 should be removed
        assert timestampLength + 4 * stepLength == bytes.length;

        // verify that the other nodes are unmodified
        assert    5 == FBUtilities.byteArrayToInt(bytes,  timestampLength + 0*stepLength);
        assert 912L == FBUtilities.byteArrayToLong(bytes, timestampLength + 0*stepLength + idLength);
        assert 132L == FBUtilities.byteArrayToLong(bytes, timestampLength + 0*stepLength + idLength + incrCountLength);

        assert   3 == FBUtilities.byteArrayToInt(bytes,  timestampLength + 1*stepLength);
        assert 35L == FBUtilities.byteArrayToLong(bytes, timestampLength + 1*stepLength + idLength);
        assert 23L == FBUtilities.byteArrayToLong(bytes, timestampLength + 1*stepLength + idLength + incrCountLength);

        assert   6 == FBUtilities.byteArrayToInt(bytes,  timestampLength + 2*stepLength);
        assert 15L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength);
        assert 11L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength + incrCountLength);

        assert   7 == FBUtilities.byteArrayToInt(bytes,  timestampLength + 3*stepLength);
        assert  1L == FBUtilities.byteArrayToLong(bytes, timestampLength + 3*stepLength + idLength);
        assert  0L == FBUtilities.byteArrayToLong(bytes, timestampLength + 3*stepLength + idLength + incrCountLength);
    }

    @Test
    public void testSerializeDeserialize() throws IOException, UnknownHostException
    {
        StandardCounterClock clock = new StandardCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(912L), FBUtilities.toByteArray(132L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(35L),  FBUtilities.toByteArray(23L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(15L),  FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(6L),   FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L),   FBUtilities.toByteArray(0L)
            ));

        // size
        DataOutputBuffer bufOut = new DataOutputBuffer();
        StandardCounterClock.SERIALIZER.serialize(clock, bufOut);

        assert bufOut.getLength() == clock.size();

        // equality
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        StandardCounterClock deserialized = (StandardCounterClock)StandardCounterClock.SERIALIZER.deserialize(new DataInputStream(bufIn));

        assert 0 == FBUtilities.compareByteArrays(clock.context(), deserialized.context());

        clock = new StandardCounterClock();
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(5)),  912L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(5)), -132L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(3)),   35L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(3)),  -23L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(6)),   15L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(6)),  -11L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(9)),    6L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(9)),   -4L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(7)),    1L);

        // size
        bufOut = new DataOutputBuffer();
        StandardCounterClock.SERIALIZER.serialize(clock, bufOut);

        assert bufOut.getLength() == clock.size();

        // equality
        bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        deserialized = (StandardCounterClock)StandardCounterClock.SERIALIZER.deserialize(new DataInputStream(bufIn));

        assert 0 == FBUtilities.compareByteArrays(clock.context(), deserialized.context());
    }
}
