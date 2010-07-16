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
package org.apache.cassandra.db.clock;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.clock.IContext;
import org.apache.cassandra.db.clock.IncrementCounterContext;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class IncrementCounterContextTest
{
    private static final IncrementCounterContext icc = new IncrementCounterContext();

    private static final int HEADER_LENGTH;

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;
    private static final int countLength;

    private static final int stepLength;
    private static final int defaultEntries;

    static
    {
        HEADER_LENGTH  = IncrementCounterContext.HEADER_LENGTH; // 2x size of long + size of int

        idAddress      = FBUtilities.getLocalAddress();
        id             = idAddress.getAddress();
        idLength       = 4; // size of int
        countLength    = 8; // size of long
        stepLength     = idLength + countLength;

        defaultEntries = 10;
    }

    @Test
    public void testCreate()
    {
        long start = System.currentTimeMillis();

        byte[] context = icc.create();
        assert context.length == HEADER_LENGTH;

        long created = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= created);
        assert (created <= System.currentTimeMillis());
    }

    @Test
    public void testUpdateEmpty()
    {
        byte[] context;

        // update
        context = icc.create();
        context = icc.update(context, idAddress);

        assert context.length == (HEADER_LENGTH + stepLength);
        assert FBUtilities.compareByteSubArrays(
                context,
                HEADER_LENGTH,
                id,
                0,
                idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                HEADER_LENGTH + idLength,
                FBUtilities.toByteArray(1L),
                0,
                countLength) == 0;

        // update w/ delta
        context = icc.create();
        context = icc.update(context, idAddress, 3L);

        assert context.length == (HEADER_LENGTH + stepLength);
        assert FBUtilities.compareByteSubArrays(
                context, HEADER_LENGTH, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context, (HEADER_LENGTH + idLength),
                FBUtilities.toByteArray(3L), 0,
                countLength) == 0;
    }

    @Test
    public void testUpdatePresent()
    {
        byte[] context;

        long start = System.currentTimeMillis();

        // update
        context = icc.create();
        context = icc.update(context, idAddress);

        long created = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= created) && (created <= System.currentTimeMillis());

        context = icc.update(context, idAddress);
        assert context.length == (HEADER_LENGTH + stepLength);
        assert 2L == FBUtilities.byteArrayToLong(context, HEADER_LENGTH + idLength);

        try
        {
            Thread.sleep(10);
        }
        catch (InterruptedException e)
        {
            assert false : "test requires a pause...";
        }

        // update w/ delta
        context = icc.update(context, idAddress, 25L);

        long created2 = FBUtilities.byteArrayToLong(context, 0);
        assert (created <= created2) && (created2 <= System.currentTimeMillis());

        assert context.length == (HEADER_LENGTH + stepLength);
        assert 27L == FBUtilities.byteArrayToLong(context, HEADER_LENGTH + idLength);
    }

    @Test
    public void testUpdatePresentReorder() throws UnknownHostException
    {
        byte[] context;

        context = new byte[HEADER_LENGTH + (stepLength * defaultEntries)];

        for (int i = 0; i < defaultEntries - 1; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }
        icc.writeElementAtStepOffset(
            context,
            (defaultEntries - 1),
            id,
            1L);

        context = icc.update(context, idAddress, 10L);

        assert context.length == (HEADER_LENGTH + (stepLength * defaultEntries));
        assert 11L == FBUtilities.byteArrayToLong(context, HEADER_LENGTH + idLength);
        for (int i = 1; i < defaultEntries; i++)
        {
            int offset = HEADER_LENGTH + (i * stepLength);
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
        }
    }

    @Test
    public void testUpdateNotPresent()
    {
        byte[] context = new byte[HEADER_LENGTH + (stepLength * 2)];

        for (int i = 0; i < 2; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }

        context = icc.update(context, idAddress, 328L);

        assert context.length == (HEADER_LENGTH + (stepLength * 3));
        assert 328L == FBUtilities.byteArrayToLong(context, HEADER_LENGTH + idLength);
        for (int i = 1; i < 3; i++)
        {
            int offset = HEADER_LENGTH + (i * stepLength);
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
            assert  1L == FBUtilities.byteArrayToLong(context, offset + idLength);
        }
    }

    @Test
    public void testSwapElement()
    {
        byte[] context = new byte[HEADER_LENGTH + (stepLength * 3)];

        for (int i = 0; i < 3; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }
        icc.swapElement(context, HEADER_LENGTH, HEADER_LENGTH + (2*stepLength));

        assert 2 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH);
        assert 0 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + (2*stepLength));

        icc.swapElement(context, HEADER_LENGTH, HEADER_LENGTH + (1*stepLength));

        assert 1 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH);
        assert 2 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + (1*stepLength));
    }

    @Test
    public void testPartitionElements()
    {
        byte[] context = new byte[HEADER_LENGTH + stepLength * 10];

        icc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L);
        icc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L);
        icc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L);
        icc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L);
        icc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L);
        icc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L);
        icc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L);
        icc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L);

        icc.partitionElements(
            context,
            0, // left
            9, // right (inclusive)
            2  // pivot
            );

        assert 5 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 0*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 2*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 4*stepLength);
        assert 1 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 6*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 7*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 8*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(context, HEADER_LENGTH + 9*stepLength);
    }

    @Test
    public void testSortElementsById()
    {
        byte[] context = new byte[HEADER_LENGTH + (stepLength * 10)];

        icc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L);
        icc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L);
        icc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L);
        icc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L);
        icc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L);
        icc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L);
        icc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L);
        icc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L);
        
        byte[] sorted = icc.sortElementsById(context);

        assert 1 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 0*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 2*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 4*stepLength);
        assert 5 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 6*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 7*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 8*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(sorted, HEADER_LENGTH + 9*stepLength);
    }

    @Test
    public void testCompare()
    {
        byte[] left;
        byte[] right;

        // equality:
        //   left:  no local timestamp
        //   right: no local timestamp
        left  = Util.concatByteArrays(FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L));
        right = Util.concatByteArrays(FBUtilities.toByteArray(1L), FBUtilities.toByteArray(0L));

        assert ClockRelationship.EQUAL ==
            icc.compare(left, right);

        // equality:
        //   left, right: local timestamps equal
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            );

        assert ClockRelationship.EQUAL ==
            icc.compare(left, right);

        // greater than:
        //   left:  local timestamp
        //   right: no local timestamp
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            );

        assert ClockRelationship.GREATER_THAN ==
            icc.compare(left, right);

        // greater than:
        //   left's local timestamp > right's local timestamp
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            );

        assert ClockRelationship.GREATER_THAN ==
            icc.compare(left, right);

        // less than:
        //   left:  no local timestamp
        //   right: local timestamp
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            );

        assert ClockRelationship.LESS_THAN ==
            icc.compare(left, right);

        // less than:
        //   left's local timestamp < right's local timestamp
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(122L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(0),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            );

        assert ClockRelationship.LESS_THAN ==
            icc.compare(left, right);
    }

    @Test
    public void testDiff()
    {
        byte[] left = new byte[HEADER_LENGTH + (3 * stepLength)];
        byte[] right;

        // equality: equal nodes, all counts same
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);
        right = ArrayUtils.clone(left);

        assert ClockRelationship.EQUAL ==
            icc.diff(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  3L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  1L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 0L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert ClockRelationship.GREATER_THAN ==
            icc.diff(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  3L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  2L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  1L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 0L);

        assert ClockRelationship.LESS_THAN ==
            icc.diff(left, right);

        // greater than: equal nodes, but left has higher counts
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert ClockRelationship.GREATER_THAN ==
            icc.diff(left, right);

        // less than: equal nodes, but right has higher counts
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 3L);

        assert ClockRelationship.LESS_THAN ==
            icc.diff(left, right);

        // disjoint: right and left have disjoint node sets
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(2),  1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(12), 1L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 2L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: left has more nodes, but lower counts
        left = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  2L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  3L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  1L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 1L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);
        
        // disjoint: left has less nodes, but higher counts
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L);

        right = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  4L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: mixed nodes and counts
        left = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L);

        right = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  4L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(7), 2L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(9), 2L);

        right = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 3L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L);

        assert ClockRelationship.DISJOINT ==
            icc.diff(left, right);
    }

    @Test
    public void testMergeReconcile()
    {
        // note:
        //   reconcile path; write flag not set
        //     local counts aggregated
        //     remote counts are reconciled (i.e. take max)

        List<byte[]> contexts;
        contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(5), 5L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        icc.writeElementAtStepOffset(
            bytes,
            2,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        byte[] merged;
        merged = icc.merge(contexts);

        contexts = new ArrayList<byte[]>();
        contexts.add(merged);

        bytes = new byte[HEADER_LENGTH + (1 * stepLength)];
        icc.setFlags(bytes, icc.FLAG_WRITE);
        icc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            4L);
        contexts.add(bytes);

        merged = icc.merge(contexts);

        // 127.0.0.1: 12 : 9 + 4
        // 0.0.0.5:    5 : 5
        // 0.0.0.4:    4 : max(3, 4)
        // 0.0.0.2:    2 : 2
        // 0.0.0.1:    1 : 1

        // flags reset
        assert 0 == icc.getFlags(merged);

        // local node id's counts are aggregated
        assert 0  == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged, HEADER_LENGTH + 0*stepLength,
            4);
        assert 13L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 0*stepLength + idLength);

        // remote node id counts are reconciled (i.e. take max)
        assert 5  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 1*stepLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 1*stepLength + idLength);

        assert 4  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 2*stepLength);
        assert 4L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 2*stepLength + idLength);

        assert 2  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 3*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 3*stepLength + idLength);

        assert 1  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 4*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 4*stepLength + idLength);
    }

    @Test
    public void testMergeInitialWrite()
    {
        // note:
        //   initial write path; write flag set
        //     aggregate all counts

        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.setFlags(bytes, icc.FLAG_WRITE);
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.setFlags(bytes, icc.FLAG_WRITE);
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(5), 5L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        icc.writeElementAtStepOffset(
            bytes,
            2,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        byte[] merged = icc.merge(contexts);

        // 127.0.0.1: 12 : 3 + 9
        // 0.0.0.4:    7 : 3 + 4
        // 0.0.0.5:    5 : 5
        // 0.0.0.2:    2 : 2
        // 0.0.0.1:    1 : 1

        // flags reset
        assert 0 == icc.getFlags(merged);

        // all node id counts are aggregated
        assert 0  == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged, HEADER_LENGTH + 0*stepLength,
            4);
        assert 12L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 0*stepLength + idLength);

        assert 4  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 1*stepLength);
        assert 7L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 1*stepLength + idLength);

        assert 5  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 2*stepLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 2*stepLength + idLength);

        assert 2  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 3*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 3*stepLength + idLength);

        assert 1  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 4*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 4*stepLength + idLength);
    }

    @Test
    public void testMergeInitialWriteAndReconcile()
    {
        // note:
        //   initial write path; write flag set
        //     aggregate all counts
        //   reconcile path; write flag not set
        //     local counts aggregated
        //     remote counts are reconciled (i.e. take max)

        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(5), 4L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        icc.writeElementAtStepOffset(
            bytes,
            2,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.setFlags(bytes, icc.FLAG_WRITE);
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(5), 5L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        icc.writeElementAtStepOffset(
            bytes,
            2,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.setFlags(bytes, icc.FLAG_WRITE);
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 9L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(3), 11L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 20L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        byte[] merged = icc.merge(contexts);

        //                 reconcile: + merged:
        // 127.0.0.1: 30 : max(3, 9)  + 9 + 9
        // 0.0.0.4:   28 : max(3, 4)  + 4 + 20
        // 0.0.0.3:   11 :              11
        // 0.0.0.1:   10 : max(1)     + 9
        // 0.0.0.5:    9 : 4          + 5
        // 0.0.0.2:    2 : 2

        // flags reset
        assert 0 == icc.getFlags(merged);

        // all node id counts are aggregated
        assert 4   == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 0*stepLength);
        assert 28L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 0*stepLength + idLength);

        assert 0   == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged, HEADER_LENGTH + 1*stepLength,
            4);
        assert 27L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 1*stepLength + idLength);

        assert 3   == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 2*stepLength);
        assert 11L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 2*stepLength + idLength);

        assert 1   == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 3*stepLength);
        assert 10L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 3*stepLength + idLength);

        assert 5  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 4*stepLength);
        assert 9L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 4*stepLength + idLength);

        assert 2  == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 5*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 5*stepLength + idLength);
    }

    @Test
    public void testTotal()
    {
        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[HEADER_LENGTH + (4 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L);
        contexts.add(bytes);

        bytes = new byte[HEADER_LENGTH + (3 * stepLength)];
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 5L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        icc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            9L);
        contexts.add(bytes);

        byte[] merged = icc.merge(contexts);

        // 127.0.0.1: 9
        // 0.0.0.1:   1
        // 0.0.0.2:   2
        // 0.0.0.4:   4
        // 0.0.0.5:   5

        assert 21L == FBUtilities.byteArrayToLong(icc.total(merged));
    }

    @Test
    public void testFlags()
    {
        byte[] bytes = new byte[HEADER_LENGTH];

        icc.setFlags(bytes, 0);
        int flags = icc.getFlags(bytes);
        assert (icc.FLAG_WRITE & flags) == 0;

        icc.setFlags(bytes, flags | icc.FLAG_WRITE);
        flags = icc.getFlags(bytes);
        assert (icc.FLAG_WRITE & flags) == 1;
    }
}
