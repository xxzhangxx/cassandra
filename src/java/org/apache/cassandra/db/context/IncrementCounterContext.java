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
package org.apache.cassandra.db.context;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import org.apache.log4j.Logger;

/**
 * An implementation of a distributed increment-only counter context.
 *
 * The data structure is:
 *   1) timestamp, and
 *   2) a list of (node id, count) pairs.
 *
 * On update:
 *   1) update timestamp to max(timestamp, local time), and
 *   2) the node updating the value will increment its associated content.
 *
 * The aggregated count can then be determined by rolling up all the counts from each
 * (node id, count) pair.  NOTE: only a given node id may increment its associated
 * count and care must be taken that (node id, count) pairs are correctly made
 * consistent.
 */
public class IncrementCounterContext implements IContext
{
    private static final int timestampLength = 8; // long

    private static final byte[] id;
    private static final int idLength;
    private static final FBUtilities.ByteArrayWrapper idWrapper;

    private static final int countLength = 8; // long

    private static final int stepLength; // length: id + count

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final IncrementCounterContext incrementCounterContext = new IncrementCounterContext();
    }

    static
    {
        id = FBUtilities.getLocalAddress().getAddress();
        idLength = id.length;
        idWrapper = new FBUtilities.ByteArrayWrapper(id);

        stepLength = idLength + countLength;
    }

    public static IncrementCounterContext instance()
    {
        return LazyHolder.incrementCounterContext;
    }

    public static byte[] getId()
    {
        return id;
    }

    /**
     * Creates an initial counter context.
     *
     * @return an empty counter context.
     */
    public byte[] create()
    {
        return FBUtilities.toByteArray(System.currentTimeMillis());
    }

    // write a tuple (node id, count) at the front
    protected static void writeElement(byte[] context, byte[] id_, long count)
    {
        writeElementAtStepOffset(context, 0, id_, count);
    }

    // write a tuple (node id, count) at step offset
    protected static void writeElementAtStepOffset(byte[] context, int stepOffset, byte[] id_, long count)
    {
        int offset = timestampLength + (stepOffset * stepLength);
        System.arraycopy(id_, 0, context, offset, idLength);
        FBUtilities.copyIntoBytes(context, offset + idLength, count);
    }

    /**
     * Updates a counter context for this node's id.
     *
     * @param context
     *            counter context
     * @param node
     *            InetAddress of node to update.
     * @return the updated version vector.
     */
    public byte[] update(byte[] context, InetAddress node)
    {
        return update(context, node, 1);
    }

    public byte[] update(byte[] context, InetAddress node, long delta)
    {
        // update timestamp
        if (context.length == 0)
        {
            context = new byte[timestampLength];
        }
        FBUtilities.copyIntoBytes(context, 0, System.currentTimeMillis());

        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: increment count, shift to front
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);

            System.arraycopy(
                context,
                timestampLength,
                context,
                timestampLength + stepLength,
                offset - timestampLength);
            writeElement(context, nodeId, count + delta);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];

        System.arraycopy(previous, 0, context, 0, timestampLength);
        writeElement(context, nodeId, delta);
        System.arraycopy(
            previous,
            timestampLength,
            context,
            timestampLength + stepLength,
            previous.length - timestampLength);

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
        int leftOffset  = timestampLength + (left       * stepLength);
        int rightOffset = timestampLength + (right      * stepLength);
        int pivotOffset = timestampLength + (pivotIndex * stepLength);

        byte[] pivotValue = ArrayUtils.subarray(context, pivotOffset, pivotOffset + stepLength);
        swapElement(context, pivotOffset, rightOffset);
        int storeOffset = leftOffset;
        for (int i = leftOffset; i < rightOffset; i += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, i, pivotValue, 0, stepLength) <= 0)
            {
                swapElement(context, i, storeOffset);
                storeOffset += stepLength;
            }
        }
        swapElement(context, storeOffset, rightOffset);
        return (storeOffset - timestampLength) / stepLength;
    }

    // quicksort helper
    protected static void sortElementsByIdHelper(byte[] context, int left, int right)
    {
        if (right <= left) return;

        int pivotIndex = (left + right) / 2;
        int pivotIndexNew = partitionElements(context, left, right, pivotIndex);
        sortElementsByIdHelper(context, left, pivotIndexNew - 1);
        sortElementsByIdHelper(context, pivotIndexNew + 1, right);
    }

    // quicksort context by id
    protected static byte[] sortElementsById(byte[] context)
    {
        assert 0 == ((context.length - timestampLength) % stepLength) : "context size is not correct.";
        sortElementsByIdHelper(
            context,
            0,
            (int)((context.length - timestampLength) / stepLength) - 1);
        return context;
    }

    /**
     * Determine the last modified relationship between two contexts.
     *
     * Strategy:
     *  compare highest timestamp between contexts.
     *
     * @param left
     *            counter context.
     * @param right
     *            counter context.
     * @return the ContextRelationship between the contexts.
     */
    public ContextRelationship compare(byte[] left, byte[] right)
    {
        long leftTimestamp  = FBUtilities.byteArrayToLong(left,  0);
        long rightTimestamp = FBUtilities.byteArrayToLong(right, 0);
        
        if (leftTimestamp < rightTimestamp)
        {
            return ContextRelationship.LESS_THAN;
        }
        else if (leftTimestamp == rightTimestamp)
        {
            return ContextRelationship.EQUAL;
        }
        return ContextRelationship.GREATER_THAN;
    }

    /**
     * Determine the count relationship between two contexts.
     *
     * Strategy:
     *  compare node count values (like a version vector).
     *
     * @param left
     *            counter context.
     * @param right
     *            counter context.
     * @return the ContextRelationship between the contexts.
     */
    public ContextRelationship diff(byte[] left, byte[] right)
    {
        left  = sortElementsById(left);
        right = sortElementsById(right);

        ContextRelationship relationship = ContextRelationship.EQUAL;

        int leftIndex  = timestampLength;
        int rightIndex = timestampLength;
        while (leftIndex < left.length && rightIndex < right.length)
        {
            // compare id bytes
            int compareId = FBUtilities.compareByteSubArrays(left,  leftIndex,
                                                             right, rightIndex,
                                                             idLength);
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
                    if (relationship == ContextRelationship.EQUAL) {
                        relationship = ContextRelationship.GREATER_THAN;
                    }
                    else if (relationship == ContextRelationship.GREATER_THAN)
                    {
                        continue;
                    }
                    else // relationship == ContextRelationship.LESS_THAN
                    {
                        return ContextRelationship.DISJOINT;
                    }
                }
                else // leftCount < rightCount
                {
                    if (relationship == ContextRelationship.EQUAL) {
                        relationship = ContextRelationship.LESS_THAN;
                    }
                    else if (relationship == ContextRelationship.GREATER_THAN)
                    {
                        return ContextRelationship.DISJOINT;
                    }
                    else // relationship == ContextRelationship.LESS_THAN
                    {
                        continue;
                    }
                }
            }
            else if (compareId > 0)
            {
                // only advance the right context
                rightIndex += stepLength;

                if (relationship == ContextRelationship.EQUAL) {
                    relationship = ContextRelationship.LESS_THAN;
                }
                else if (relationship == ContextRelationship.GREATER_THAN)
                {
                    return ContextRelationship.DISJOINT;
                }
                else // relationship == ContextRelationship.LESS_THAN
                {
                    continue;
                }
            }
            else // compareId < 0
            {
                // only advance the left context
                leftIndex += stepLength;

                if (relationship == ContextRelationship.EQUAL) {
                    relationship = ContextRelationship.GREATER_THAN;
                }
                else if (relationship == ContextRelationship.GREATER_THAN)
                {
                    continue;
                }
                else // relationship == ContextRelationship.LESS_THAN
                {
                    return ContextRelationship.DISJOINT;
                }
            }
        }

        // check final lengths
        if (leftIndex < left.length)
        {
            if (relationship == ContextRelationship.EQUAL) {
                return ContextRelationship.GREATER_THAN;
            }
            else if (relationship == ContextRelationship.LESS_THAN)
            {
                return ContextRelationship.DISJOINT;
            }
        }
        else if (rightIndex < right.length)
        {
            if (relationship == ContextRelationship.EQUAL) {
                return ContextRelationship.LESS_THAN;
            }
            else if (relationship == ContextRelationship.GREATER_THAN)
            {
                return ContextRelationship.DISJOINT;
            }
        }

        return relationship;
    }

    /**
     * Return a context w/ an aggregated count for each node id.
     *
     * @param contexts
     *            a list of contexts to be merged
     */
    public byte[] merge(List<byte[]> contexts)
    {
        // strategy:
        //   1) take highest timestamp
        //   2) map id -> count
        //      a) local id:  sum counts; keep highest timestamp
        //      b) remote id: keep highest count (reconcile)
        //   3) create a context from sorted array
        long highestTimestamp = Long.MIN_VALUE;
        Map<FBUtilities.ByteArrayWrapper, Long> contextsMap =
            new HashMap<FBUtilities.ByteArrayWrapper, Long>();
        for (byte[] context : contexts)
        {
            // take highest timestamp
            highestTimestamp = Math.max(FBUtilities.byteArrayToLong(context, 0), highestTimestamp);

            // map id -> count
            for (int offset = timestampLength; offset < context.length; offset += stepLength)
            {
                FBUtilities.ByteArrayWrapper id = new FBUtilities.ByteArrayWrapper(
                        ArrayUtils.subarray(context, offset, offset + idLength));
                long count = FBUtilities.byteArrayToLong(context, offset + idLength);

                if (!contextsMap.containsKey(id))
                {
                    contextsMap.put(id, count);
                    continue;
                }

                // local id: sum counts
                if (this.idWrapper.equals(id))
                {
                    contextsMap.put(id, count + (Long)contextsMap.get(id));
                    continue;
                }

                // remote id: keep highest count
                if ((Long)contextsMap.get(id) < count)
                {
                    contextsMap.put(id, count);
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
        byte[] merged = new byte[timestampLength + (length * stepLength)];
        FBUtilities.copyIntoBytes(merged, 0, highestTimestamp);
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
        sb.append("{");
        sb.append(FBUtilities.byteArrayToLong(context, 0));
        sb.append(" + [");
        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            if (offset != timestampLength)
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
        sb.append("]}");
        return sb.toString();
    }

    // return an aggregated count across all node ids
    public byte[] total(byte[] context)
    {
        long total = 0;

        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);
            total += count;
        }

        return FBUtilities.toByteArray(total);
    }

    // remove the count for a given node id
    public byte[] cleanNodeCounts(byte[] context, InetAddress node)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: remove node count
            byte[] truncatedContext = new byte[context.length - stepLength];
            System.arraycopy(context, 0, truncatedContext, 0, offset);
            System.arraycopy(
                context,
                offset + stepLength,
                truncatedContext,
                offset,
                context.length - (offset + stepLength));
            return truncatedContext;
        }

        return context;
    }
}
