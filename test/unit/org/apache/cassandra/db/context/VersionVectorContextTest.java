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
package org.apache.cassandra.db.context;

import java.net.InetAddress;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class VersionVectorContextTest
{
    private static final VersionVectorContext vvc = new VersionVectorContext();

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;
    private static final int countLength;
    private static final int stepLength;
    private static final int defaultEntries;

    static
    {
        idAddress       = FBUtilities.getLocalAddress();
        id              = idAddress.getAddress();
        idLength        = 4; // size of int
        countLength     = 8; // size of long
        stepLength      = idLength + countLength;

        defaultEntries  = 10;
    }

    @Test
    public void testCreate()
    {
        byte[] context = vvc.create();
        assert context.length == 0;
    }

    @Test
    public void testUpdateEmpty()
    {
        byte[] context = vvc.create();
        context = vvc.update(context, idAddress);

        assert context.length == stepLength;
        assert FBUtilities.compareByteSubArrays(
                context, 0, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context, idLength,
                FBUtilities.toByteArray(1L), 0,
                countLength) == 0;
    }

    @Test
    public void testUpdatePresent()
    {
        byte[] context = vvc.create();
        context = vvc.update(context, idAddress);

        context = vvc.update(context, idAddress);
        assert context.length == stepLength;
        assert 2L == FBUtilities.byteArrayToLong(context, idLength);
    }

    @Test
    public void testUpdatePresentReorder()
    {
        byte[] context = new byte[stepLength * defaultEntries];

        for (int i = 0; i < defaultEntries - 1; i++)
        {
            vvc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }
        vvc.writeElementAtStepOffset(
            context,
            (defaultEntries - 1),
            id,
            1L);

        context = vvc.update(context, idAddress);

        assert context.length == stepLength * defaultEntries;
        assert 2L == FBUtilities.byteArrayToLong(context, idLength);
        for (int i = 1; i < defaultEntries; i++)
        {
            int offset = i * stepLength;
            assert i-1 == FBUtilities.byteArrayToInt(context, offset);
            assert 1L  == FBUtilities.byteArrayToLong(context, offset + idLength);
        }
    }

    @Test
    public void testUpdateNotPresent()
    {
        byte[] context = new byte[stepLength * 2];

        for (int i = 0; i < 2; i++)
        {
            vvc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }

        context = vvc.update(context, idAddress);

        assert context.length == stepLength * 3;
        assert 1L == FBUtilities.byteArrayToLong(context, idLength);
        for (int i = 1; i < 3; i++)
        {
            int offset = i * stepLength;
            assert i-1 == FBUtilities.byteArrayToInt(context, offset);
            assert 1L  == FBUtilities.byteArrayToLong(context, offset + idLength);
        }
    }

    @Test
    public void testSwapElement()
    {
        byte[] context = new byte[stepLength * 3];

        for (int i = 0; i < 3; i++)
        {
            vvc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L);
        }
        vvc.swapElement(context, 0, 2*stepLength);

        assert 2 == FBUtilities.byteArrayToInt(context, 0);
        assert 0 == FBUtilities.byteArrayToInt(context, 2*stepLength);

        vvc.swapElement(context, 0, 1*stepLength);

        assert 1 == FBUtilities.byteArrayToInt(context, 0);
        assert 2 == FBUtilities.byteArrayToInt(context, 1*stepLength);
    }

    @Test
    public void testPartitionElements()
    {
        byte[] context = new byte[stepLength * 10];

        vvc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L);
        vvc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L);
        vvc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L);
        vvc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L);
        vvc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L);
        vvc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L);
        vvc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L);
        vvc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L);
        vvc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L);

        vvc.partitionElements(context,
                0*stepLength,  // left
                9*stepLength,  // right (inclusive)
                2*stepLength); // pivot

        assert 5 == FBUtilities.byteArrayToInt(context, 0*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, 2*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(context, 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(context, 4*stepLength);
        assert 1 == FBUtilities.byteArrayToInt(context, 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(context, 6*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(context, 7*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(context, 8*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(context, 9*stepLength);
    }

    @Test
    public void testSortElementsById()
    {
        byte[] context = new byte[stepLength * 10];

        vvc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L);
        vvc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L);
        vvc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L);
        vvc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L);
        vvc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L);
        vvc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L);
        vvc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L);
        vvc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L);
        vvc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L);
        
        byte[] sorted = vvc.sortElementsById(context);

        assert 1 == FBUtilities.byteArrayToInt(sorted, 0*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(sorted, 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, 2*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(sorted, 4*stepLength);
        assert 5 == FBUtilities.byteArrayToInt(sorted, 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(sorted, 6*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(sorted, 7*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(sorted, 8*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(sorted, 9*stepLength);
    }

    @Test
    public void testCompare()
    {
        byte[] left = new byte[3 * stepLength];
        byte[] right;

        // equality: equal nodes, all counts same
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);
        right = ArrayUtils.clone(left);

        assert IContext.ContextRelationship.EQUAL ==
            vvc.compare(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);
        vvc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 0L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            vvc.compare(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);
        vvc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 0L);

        assert IContext.ContextRelationship.LESS_THAN ==
            vvc.compare(left, right);

        // greater than: equal nodes, but left has higher counts
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            vvc.compare(left, right);

        // less than: equal nodes, but right has higher counts
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 3L);

        assert IContext.ContextRelationship.LESS_THAN ==
            vvc.compare(left, right);

        // disjoint: right and left have disjoint node sets
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(2), 1L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(12), 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 2L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        // disjoint: left has more nodes, but lower counts
        left = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 2L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L);
        vvc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 1L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);
        
        // disjoint: left has less nodes, but higher counts
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L);

        right = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L);
        vvc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        // disjoint: mixed nodes and counts
        left = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L);

        right = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L);
        vvc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);

        left = new byte[4 * stepLength];
        vvc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L);
        vvc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L);
        vvc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(7), 2L);
        vvc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(9), 2L);

        right = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L);
        vvc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 3L);
        vvc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L);

        assert IContext.ContextRelationship.DISJOINT ==
            vvc.compare(left, right);
    }

    @Test
    public void testMerge()
    {
        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L);
        vvc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L);
        vvc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(3), 3L);
        contexts.add(bytes);

        bytes = new byte[3 * stepLength];
        vvc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 5L);
        vvc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L);
        vvc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(3), 9L);
        contexts.add(bytes);

        byte[] merged = vvc.merge(contexts);

        assert 3 == FBUtilities.byteArrayToInt(merged, 0*stepLength);
        assert 5 == FBUtilities.byteArrayToInt(merged, 1*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(merged, 2*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(merged, 3*stepLength);
        assert 1 == FBUtilities.byteArrayToInt(merged, 4*stepLength);

        assert 9L == FBUtilities.byteArrayToLong(merged, 0*stepLength + idLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, 1*stepLength + idLength);
        assert 4L == FBUtilities.byteArrayToLong(merged, 2*stepLength + idLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, 3*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, 4*stepLength + idLength);
    }
}
