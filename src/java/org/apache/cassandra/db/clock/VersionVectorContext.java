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

package org.apache.cassandra.db.clock;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.utils.FBUtilities;

/**
 * An implementation of Version Vectors.
 *
 * The data structure is a list of (node id, count) pairs.  As a value is updated,
 * the node updating the value will increment its associated count in the VV.
 * Version ordering can then be automatically determined by comparing whether
 * one VV dominates another VV wrt all (node id, count) pairs.  If neither VV
 * completely dominates the other, then a conflict has been discovered and
 * must be resolved, externally.
 */
public class VersionVectorContext implements IContext
{
    private static final int idLength;
    private static final int countLength = DBConstants.longSize_;
    private static final int stepLength; // length: id + count

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final VersionVectorContext versionVectorContext = new VersionVectorContext();
    }

    static
    {
        idLength   = FBUtilities.getLocalAddress().getAddress().length;
        stepLength = idLength + countLength;
    }

    public static VersionVectorContext instance()
    {
        return LazyHolder.versionVectorContext;
    }

    /**
     * Creates an initial version vector.
     *
     * @return an empty version vector.
     */
    public byte[] create()
    {
        return ArrayUtils.EMPTY_BYTE_ARRAY;
    }

    // write a tuple (node id, count) at the front
    protected static void writeElement(byte[] context, byte[] id, long count)
    {
        writeElementAtStepOffset(context, 0, id, count);
    }

    // write a tuple (node id, count) at step offset
    protected static void writeElementAtStepOffset(byte[] context, int stepOffset, byte[] id, long count)
    {
        int offset = stepOffset * stepLength;
        System.arraycopy(id, 0, context, offset, idLength);
        FBUtilities.copyIntoBytes(context, offset + idLength, count);
    }

    /**
     * Updates a version vector with this node's id.
     *
     * @param context
     *            version vector.
     * @param node
     *            InetAddress of node to update.
     * @return the updated version vector.
     */
    public byte[] update(byte[] context, InetAddress node)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: increment count, shift to front
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);

            System.arraycopy(context, 0, context, stepLength, offset);
            writeElement(context, nodeId, count+1L);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];

        writeElement(context, nodeId, 1L);
        System.arraycopy(previous, 0, context, stepLength, previous.length);

        return context;
    }

    // swap bytes of step length in context
    protected static void swapElement(byte[] context, int left, int right)
    {
        if (left == right) return;

        byte temp;
        for (int i = 0; i < stepLength; i++)
        {
            temp = context[left+i];
            context[left+i] = context[right+i];
            context[right+i] = temp;
        }
    }

    // partition bytes of step length in context (for quicksort)
    protected static int partitionElements(byte[] context, int left, int right, int pivotIndex)
    {
        byte[] pivotValue = ArrayUtils.subarray(context, pivotIndex, pivotIndex+stepLength);
        swapElement(context, pivotIndex, right);
        int storeIndex = left;
        for (int i = left; i < right; i += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, i, pivotValue, 0, stepLength) <= 0)
            {
                swapElement(context, i, storeIndex);
                storeIndex += stepLength;
            }
        }
        swapElement(context, storeIndex, right);
        return storeIndex;
    }

    // quicksort helper
    protected static void sortElementsByIdHelper(byte[] context, int left, int right)
    {
        if (right <= left) return;

        int pivotIndex = (left + right) / 2;
        pivotIndex -= pivotIndex % stepLength;
        int pivotIndexNew = partitionElements(context, left, right, pivotIndex);
        sortElementsByIdHelper(context, left, pivotIndexNew - stepLength);
        sortElementsByIdHelper(context, pivotIndexNew + stepLength, right);
    }

    // quicksort context by id
    protected static byte[] sortElementsById(byte[] context)
    {
        if ((context.length % stepLength) != 0)
        {
            throw new IllegalArgumentException("The context array isn't a multiple of step length.");
        }

        byte[] sorted = ArrayUtils.clone(context);
        sortElementsByIdHelper(sorted, 0, sorted.length - stepLength);
        return sorted;
    }

    /**
     * Determine the version relationship between two contexts.
     *
     * Strategy:
     *  Sort contexts by id.
     *  Advance through both contexts and handle every case.
     *
     * @param left
     *            version context.
     * @param right
     *            version context.
     * @return the ClockRelationship between the contexts.
     */
    public ClockRelationship compare(byte[] left, byte[] right)
    {
        left  = sortElementsById(left);
        right = sortElementsById(right);

        ClockRelationship relationship = ClockRelationship.EQUAL;

        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < left.length && rightIndex < right.length)
        {
            // compare id bytes
            int compareId = FBUtilities.compareByteSubArrays(left, leftIndex, right, rightIndex, idLength);
            if (compareId == 0)
            {
                long leftCount  = FBUtilities.byteArrayToLong(left,  leftIndex  + idLength);
                long rightCount = FBUtilities.byteArrayToLong(right, rightIndex + idLength);

                // advance indexes
                leftIndex  += stepLength;
                rightIndex += stepLength;

                // process count comparisons
                if (leftCount == rightCount)
                {
                    continue;
                }
                else if (leftCount > rightCount)
                {
                    if (relationship == ClockRelationship.EQUAL) {
                        relationship = ClockRelationship.GREATER_THAN;
                    }
                    else if (relationship == ClockRelationship.GREATER_THAN)
                    {
                        continue;
                    }
                    else // relationship == ClockRelationship.LESS_THAN
                    {
                        return ClockRelationship.DISJOINT;
                    }
                }
                else // leftCount < rightCount
                {
                    if (relationship == ClockRelationship.EQUAL) {
                        relationship = ClockRelationship.LESS_THAN;
                    }
                    else if (relationship == ClockRelationship.GREATER_THAN)
                    {
                        return ClockRelationship.DISJOINT;
                    }
                    else // relationship == ClockRelationship.LESS_THAN
                    {
                        continue;
                    }
                }
            }
            else if (compareId > 0)
            {
                // only advance the right context
                rightIndex += stepLength;

                if (relationship == ClockRelationship.EQUAL) {
                    relationship = ClockRelationship.LESS_THAN;
                }
                else if (relationship == ClockRelationship.GREATER_THAN)
                {
                    return ClockRelationship.DISJOINT;
                }
                else // relationship == ClockRelationship.LESS_THAN
                {
                    continue;
                }
            }
            else // compareId < 0
            {
                // only advance the left context
                leftIndex += stepLength;

                if (relationship == ClockRelationship.EQUAL) {
                    relationship = ClockRelationship.GREATER_THAN;
                }
                else if (relationship == ClockRelationship.GREATER_THAN)
                {
                    continue;
                }
                else // relationship == ClockRelationship.LESS_THAN
                {
                    return ClockRelationship.DISJOINT;
                }
            }
        }

        // check final lengths
        if (leftIndex < left.length)
        {
            if (relationship == ClockRelationship.EQUAL) {
                return ClockRelationship.GREATER_THAN;
            }
            else if (relationship == ClockRelationship.LESS_THAN)
            {
                return ClockRelationship.DISJOINT;
            }
        }
        else if (rightIndex < right.length)
        {
            if (relationship == ClockRelationship.EQUAL) {
                return ClockRelationship.LESS_THAN;
            }
            else if (relationship == ClockRelationship.GREATER_THAN)
            {
                return ClockRelationship.DISJOINT;
            }
        }

        return relationship;
    }

    /**
     * Return a context that pairwise dominates all of the contexts.
     *
     * @param contexts
     *            a list of contexts to be merged
     */
    public byte[] merge(List<byte[]> contexts)
    {
        // strategy:
        //   map id -> count (keep highest count)
        //   create an array sorted by count
        //   create a context from sorted array
        Map<FBUtilities.ByteArrayWrapper, Long> contextsMap =
            new HashMap<FBUtilities.ByteArrayWrapper, Long>();
        for (byte[] context : contexts)
        {
            for (int offset = 0; offset < context.length; offset += stepLength)
            {
                FBUtilities.ByteArrayWrapper id = new FBUtilities.ByteArrayWrapper(
                        ArrayUtils.subarray(context, offset, offset + idLength));
                long count = FBUtilities.byteArrayToLong(context, offset + idLength);

                if (!contextsMap.containsKey(id))
                {
                    contextsMap.put(id, new Long(count));
                    continue;
                }

                if (((Long)contextsMap.get(id)) < count)
                {
                    contextsMap.put(id, new Long(count));
                }
            }
        }

        List<Map.Entry<FBUtilities.ByteArrayWrapper, Long>> contextsList =
            new ArrayList<Map.Entry<FBUtilities.ByteArrayWrapper, Long>>(
                    contextsMap.entrySet());
        Collections.sort(
            contextsList,
            new Comparator<Map.Entry<FBUtilities.ByteArrayWrapper, Long>>()
            {
                public int compare(
                    Map.Entry<FBUtilities.ByteArrayWrapper, Long> e1,
                    Map.Entry<FBUtilities.ByteArrayWrapper, Long> e2)
                {
                    // reversed
                    return e2.getValue().compareTo(e1.getValue());
                }
            });

        int length = contextsList.size();
        byte[] merged = new byte[length * stepLength];
        for (int i = 0; i < length; i++)
        {
            Map.Entry<FBUtilities.ByteArrayWrapper, Long> entry = contextsList.get(i);
            writeElementAtStepOffset(
                merged,
                i,
                entry.getKey().data,
                entry.getValue().longValue());
        }
        return merged;
    }

    /**
     * Human-readable String from context.
     *
     * @param context
     *            version context.
     * @return a human-readable String of the context.
     */
    public String toString(byte[] context)
    {
        context = sortElementsById(context);

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            if (offset > 0)
            {
                sb.append(",");
            }
            sb.append("(");
            try
            {
                InetAddress address = InetAddress.getByAddress(
                            ArrayUtils.subarray(context, offset, offset + idLength));
                sb.append(address.getHostAddress());
            }
            catch (UnknownHostException uhe)
            {
                sb.append("?.?.?.?");
            }
            sb.append(", ");
            sb.append(FBUtilities.byteArrayToLong(context, offset + idLength));
            sb.append(")");
        }
        sb.append("]");
        return sb.toString();
    }
}
