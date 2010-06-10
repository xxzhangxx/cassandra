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

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class StandardCounterContextTest
{
    private static final StandardCounterContext scc = new StandardCounterContext();

    private static final int timestampLength;

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;

    private static final int incrCountLength;
    private static final int decrCountLength;

    private static final int stepLength;

    private static final int defaultEntries;

    static
    {
        timestampLength = 8; // size of long

        idAddress       = FBUtilities.getLocalAddress();
        id              = idAddress.getAddress();
        idLength        = 4; // size of int

        incrCountLength = 8; // size of long
        decrCountLength = 8; // size of long

        stepLength      = idLength + incrCountLength + decrCountLength;

        defaultEntries  = 10;
    }

    @Test
    public void testCreate()
    {
        long start = System.currentTimeMillis();

        byte[] context = scc.create();
        assert context.length == timestampLength;

        long created = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= created) && (created <= System.currentTimeMillis());
    }

    @Test
    public void testUpdateEmpty()
    {
        byte[] context;

        long start = System.currentTimeMillis();
        long timestamp;

        // update
        context = scc.create();
        context = scc.update(context, idAddress);

        assert context.length == (timestampLength + stepLength);

        timestamp = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= timestamp) && (timestamp <= System.currentTimeMillis());

        assert FBUtilities.compareByteSubArrays(
                context, timestampLength, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength,
                FBUtilities.toByteArray(1L),
                0,
                incrCountLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength + incrCountLength,
                FBUtilities.toByteArray(0L),
                0,
                decrCountLength) == 0;

        // update w/ delta
        context = scc.create();
        context = scc.update(context, idAddress, 3L);

        assert context.length == (timestampLength + stepLength);

        timestamp = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= timestamp) && (timestamp <= System.currentTimeMillis());

        assert FBUtilities.compareByteSubArrays(
                context, timestampLength, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength,
                FBUtilities.toByteArray(3L),
                0,
                incrCountLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength + incrCountLength,
                FBUtilities.toByteArray(0L),
                0,
                decrCountLength) == 0;

        // update w/ negative delta
        context = scc.create();
        context = scc.update(context, idAddress, -4L);

        assert context.length == (timestampLength + stepLength);

        timestamp = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= timestamp) && (timestamp <= System.currentTimeMillis());

        assert FBUtilities.compareByteSubArrays(
                context, timestampLength, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength,
                FBUtilities.toByteArray(0L),
                0,
                incrCountLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context,
                timestampLength + idLength + incrCountLength,
                FBUtilities.toByteArray(4L),
                0,
                decrCountLength) == 0;
    }

    @Test
    public void testUpdatePresent() throws InterruptedException
    {
        byte[] context;

        long start = System.currentTimeMillis();

        // update
        context = scc.create();
        context = scc.update(context, idAddress);

        long created = FBUtilities.byteArrayToLong(context, 0);
        assert (start <= created) && (created <= System.currentTimeMillis());

        context = scc.update(context, idAddress);
        assert context.length == (timestampLength + stepLength);
        assert 2L == FBUtilities.byteArrayToLong(context, timestampLength + idLength);
        assert 0L == FBUtilities.byteArrayToLong(context, timestampLength + idLength + incrCountLength);

        Thread.sleep(10);

        // update w/ delta
        context = scc.update(context, idAddress, 25L);

        long created2 = FBUtilities.byteArrayToLong(context, 0);
        assert (created < created2) && (created2 <= System.currentTimeMillis());

        assert context.length == (timestampLength + stepLength);
        assert 27L == FBUtilities.byteArrayToLong(context, timestampLength + idLength);
        assert  0L == FBUtilities.byteArrayToLong(context, timestampLength + idLength + incrCountLength);

        Thread.sleep(10);

        // update w/ negative delta
        context = scc.update(context, idAddress, -10L);

        long created3 = FBUtilities.byteArrayToLong(context, 0);
        assert (created2 < created3) && (created3 <= System.currentTimeMillis());

        assert context.length == (timestampLength + stepLength);
        assert 27L == FBUtilities.byteArrayToLong(context, timestampLength + idLength);
        assert 10L == FBUtilities.byteArrayToLong(context, timestampLength + idLength + incrCountLength);
    }

    @Test
    public void testUpdatePresentReorder() throws UnknownHostException
    {
        byte[] context;

        context = new byte[timestampLength + (stepLength * defaultEntries)];

        for (int i = 0; i < defaultEntries - 1; i++)
        {
            scc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                2L);
        }
        scc.writeElementAtStepOffset(
            context,
            (defaultEntries - 1),
            id,
            1L,
            2L);

        context = scc.update(context, idAddress, -10L);

        assert context.length == timestampLength + (stepLength * defaultEntries);
        assert  1L == FBUtilities.byteArrayToLong(context, timestampLength + idLength);
        assert 12L == FBUtilities.byteArrayToLong(context, timestampLength + idLength + incrCountLength);
        for (int i = 1; i < defaultEntries; i++)
        {
            int offset = timestampLength + (i * stepLength);
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
            assert  1L == FBUtilities.byteArrayToLong(context, offset + idLength);
            assert  2L == FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength);
        }
    }

    @Test
    public void testUpdateNotPresent()
    {
        byte[] context = new byte[timestampLength + (stepLength * 2)];

        for (int i = 0; i < 2; i++)
        {
            scc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                2L);
        }

        context = scc.update(context, idAddress, -328L);

        assert context.length == (timestampLength + (stepLength * 3));
        assert   0L == FBUtilities.byteArrayToLong(context, timestampLength + idLength);
        assert 328L == FBUtilities.byteArrayToLong(context, timestampLength + idLength + incrCountLength);
        for (int i = 1; i < 3; i++)
        {
            int offset = timestampLength + (i * stepLength);
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
            assert  1L == FBUtilities.byteArrayToLong(context, offset + idLength);
            assert  2L == FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength);
        }
    }

    @Test
    public void testSwapElement()
    {
        byte[] context = new byte[timestampLength + (stepLength * 3)];

        for (int i = 0; i < 3; i++)
        {
            scc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                2L);
        }
        scc.swapElement(context, timestampLength, timestampLength + (2*stepLength));

        assert 2 == FBUtilities.byteArrayToInt(context, timestampLength);
        assert 0 == FBUtilities.byteArrayToInt(context, timestampLength + (2*stepLength));

        scc.swapElement(context, timestampLength, timestampLength + (1*stepLength));

        assert 1 == FBUtilities.byteArrayToInt(context, timestampLength);
        assert 2 == FBUtilities.byteArrayToInt(context, timestampLength + (1*stepLength));
    }

    @Test
    public void testPartitionElements()
    {
        byte[] context = new byte[timestampLength + (stepLength * 10)];

        scc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L, 2L);
        scc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L, 2L);
        scc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L, 2L);
        scc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L, 2L);
        scc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L, 2L);
        scc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L, 2L);
        scc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L, 2L);
        scc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L, 2L);
        scc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L, 2L);
        scc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L, 2L);

        scc.partitionElements(
                context,
                0,  // left
                9,  // right (inclusive)
                2   // pivot
                );

        assert 5 == FBUtilities.byteArrayToInt(context, timestampLength + 0*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, timestampLength + 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, timestampLength + 2*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(context, timestampLength + 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(context, timestampLength + 4*stepLength);
        assert 1 == FBUtilities.byteArrayToInt(context, timestampLength + 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(context, timestampLength + 6*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(context, timestampLength + 7*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(context, timestampLength + 8*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(context, timestampLength + 9*stepLength);
    }

    @Test
    public void testSortElementsById()
    {
        byte[] context = new byte[timestampLength + (stepLength * 10)];

        scc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L, 2L);
        scc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L, 2L);
        scc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L, 2L);
        scc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L, 2L);
        scc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L, 2L);
        scc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L, 2L);
        scc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L, 2L);
        scc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L, 2L);
        scc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L, 2L);
        scc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L, 2L);
        
        byte[] sorted = scc.sortElementsById(context);

        assert 1 == FBUtilities.byteArrayToInt(sorted, timestampLength + 0*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(sorted, timestampLength + 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, timestampLength + 2*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, timestampLength + 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(sorted, timestampLength + 4*stepLength);
        assert 5 == FBUtilities.byteArrayToInt(sorted, timestampLength + 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(sorted, timestampLength + 6*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(sorted, timestampLength + 7*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(sorted, timestampLength + 8*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(sorted, timestampLength + 9*stepLength);
    }

    @Test
    public void testCompare()
    {
        byte[] left;
        byte[] right;

        // equality:
        left  = FBUtilities.toByteArray(1L);
        right = FBUtilities.toByteArray(1L);

        assert IContext.ContextRelationship.EQUAL ==
            scc.compare(left, right);

        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(10L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(7L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(1L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(231L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(34L)
            );

        assert IContext.ContextRelationship.EQUAL ==
            scc.compare(left, right);

        // greater than:
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(7L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(97L)
            );

        assert IContext.ContextRelationship.GREATER_THAN ==
            scc.compare(left, right);

        left = Util.concatByteArrays(
            FBUtilities.toByteArray(11L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(8L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(2L)
            );

        assert IContext.ContextRelationship.GREATER_THAN ==
            scc.compare(left, right);

        // less than:
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(6L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(8L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(4L)
            );

        assert IContext.ContextRelationship.LESS_THAN ==
            scc.compare(left, right);

        left = Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(82L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(16L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(122L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(2L)
            );

        assert IContext.ContextRelationship.LESS_THAN ==
            scc.compare(left, right);
    }

    @Test
    public void testDiff()
    {
        byte[] left = new byte[timestampLength + (3 * stepLength)];
        byte[] right;

        // equality: equal nodes, all counts same
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 3L);
        right = ArrayUtils.clone(left);

        assert IContext.ContextRelationship.EQUAL ==
            scc.diff(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  3L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  2L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  1L, 3L);
        scc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 0L, 0L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 3L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            scc.diff(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 3L);

        right = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  3L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  2L, 2L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  1L, 3L);
        scc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 0L, 0L);

        assert IContext.ContextRelationship.LESS_THAN ==
            scc.diff(left, right);

        // greater than: equal nodes, but left has higher counts
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 4L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L, 3L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 3L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            scc.diff(left, right);

        // less than: equal nodes, but right has higher counts
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L, 3L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L, 2L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 3L, 9L);

        assert IContext.ContextRelationship.LESS_THAN ==
            scc.diff(left, right);

        // disjoint: right and left have disjoint node sets
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L, 1L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L, 1L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 1L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L, 1L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(2),  1L, 1L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  1L, 1L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(12), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 2L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 2L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 2L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L, 2L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 5L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 2L, 7L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 5L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 0L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L, 3L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L, 2L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        // disjoint: left has more nodes, but lower counts
        left = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  2L, 0L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  0L, 3L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  0L, 1L);
        scc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 1L, 0L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 2L, 4L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 0L, 9L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L, 0L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);
 
        // disjoint: left has less nodes, but higher counts
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 0L, 5L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 0L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L, 0L);

        right = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  0L, 4L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L, 0L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L, 0L);
        scc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 0L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        // disjoint: mixed nodes and counts
        left = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 0L, 5L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 0L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L, 0L);

        right = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  0L, 4L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L, 0L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L, 0L);
        scc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);

        left = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L, 0L);
        scc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 0L, 2L);
        scc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(7), 0L, 2L);
        scc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(9), 2L, 0L);

        right = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L, 0L);
        scc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 0L, 3L);
        scc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L, 0L);

        assert IContext.ContextRelationship.DISJOINT ==
            scc.diff(left, right);
    }

    @Test
    public void testMerge()
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)

        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 1L);
        scc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 0L);
        scc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 0L, 3L);
        scc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            4L);
        contexts.add(bytes);

        bytes = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 3L, 5L);
        scc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L, 6L);
        scc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            3L);
        contexts.add(bytes);

        byte[] merged = scc.merge(contexts);

        // local node id's counts are aggregated
        assert 0  == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged, timestampLength + 0*stepLength,
            4);
        assert 12L == FBUtilities.byteArrayToLong(merged, timestampLength + 0*stepLength + idLength);
        assert  7L == FBUtilities.byteArrayToLong(merged, timestampLength + 0*stepLength + idLength + incrCountLength);

        // remote node id counts are reconciled (i.e. take max)
        assert 4  == FBUtilities.byteArrayToInt(merged,  timestampLength + 1*stepLength);
        assert 4L == FBUtilities.byteArrayToLong(merged, timestampLength + 1*stepLength + idLength);
        assert 6L == FBUtilities.byteArrayToLong(merged, timestampLength + 1*stepLength + idLength + incrCountLength);

        assert 5  == FBUtilities.byteArrayToInt(merged,  timestampLength + 2*stepLength);
        assert 3L == FBUtilities.byteArrayToLong(merged, timestampLength + 2*stepLength + idLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, timestampLength + 2*stepLength + idLength + incrCountLength);

        assert 2  == FBUtilities.byteArrayToInt(merged,  timestampLength + 3*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, timestampLength + 3*stepLength + idLength);
        assert 0L == FBUtilities.byteArrayToLong(merged, timestampLength + 3*stepLength + idLength + incrCountLength);

        assert 1  == FBUtilities.byteArrayToInt(merged,  timestampLength + 4*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, timestampLength + 4*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, timestampLength + 4*stepLength + idLength + incrCountLength);
    }

    @Test
    public void testTotal()
    {
        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 4L);
        scc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 5L);
        scc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L, 6L);
        scc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            9L);
        contexts.add(bytes);

        bytes = new byte[timestampLength + (3 * stepLength)];
        scc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 5L, 6L);
        scc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L, 7L);
        scc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            2L);
        contexts.add(bytes);

        byte[] merged = scc.merge(contexts);

        // 127.0.0.1: 12 (3+9) - 11 (9+2)
        // 0.0.0.1:    1       -  4
        // 0.0.0.2:    2       -  5
        // 0.0.0.4:    4       -  7
        // 0.0.0.5:    5       -  6

        assert (24L - 33L) == FBUtilities.byteArrayToLong(scc.total(merged));
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        byte[] bytes = new byte[timestampLength + (4 * stepLength)];
        scc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 8L);
        scc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 7L);
        scc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L, 6L);
        scc.writeElementAtStepOffset(bytes, 3, FBUtilities.toByteArray(8), 4L, 5L);

        assert 4  == FBUtilities.byteArrayToInt(bytes,  timestampLength + 2*stepLength);
        assert 3L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength);
        assert 6L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength + incrCountLength);

        bytes = scc.cleanNodeCounts(bytes, InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        // node: 0.0.0.4 should be removed
        assert (timestampLength + (3 * stepLength)) == bytes.length;

        // other nodes should be unaffected
        assert 1  == FBUtilities.byteArrayToInt(bytes,  timestampLength + 0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(bytes, timestampLength + 0*stepLength + idLength);
        assert 8L == FBUtilities.byteArrayToLong(bytes, timestampLength + 0*stepLength + idLength + incrCountLength);

        assert 2  == FBUtilities.byteArrayToInt(bytes,  timestampLength + 1*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(bytes, timestampLength + 1*stepLength + idLength);
        assert 7L == FBUtilities.byteArrayToLong(bytes, timestampLength + 1*stepLength + idLength + incrCountLength);

        assert 8  == FBUtilities.byteArrayToInt(bytes,  timestampLength + 2*stepLength);
        assert 4L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength);
        assert 5L == FBUtilities.byteArrayToLong(bytes, timestampLength + 2*stepLength + idLength + incrCountLength);
    }
}
