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
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.utils.FBUtilities;

import org.apache.log4j.Logger;

/**
 * An abstract implementation of a distributed counter context.
 */
public abstract class AbstractCounterContext implements IContext
{
    protected static final int timestampLength = 8; // long

    protected static final byte[] id;
    protected static final int idLength;
    protected static final FBUtilities.ByteArrayWrapper idWrapper;

    static
    {
        id = FBUtilities.getLocalAddress().getAddress();
        idLength = id.length;
        idWrapper = new FBUtilities.ByteArrayWrapper(id);
    }

    public static byte[] getId()
    {
        return id;
    }

    // step length: id + count(s) + timestamp
    protected abstract int getStepLength();

    /**
     * Creates an initial counter context.
     *
     * @return an empty counter context set to the current timestamp.
     */
    public byte[] create()
    {
        return FBUtilities.toByteArray(System.currentTimeMillis());
    }

    protected void updateTimestamp(byte[] context)
    {
        long now     = System.currentTimeMillis();
        long current = FBUtilities.byteArrayToLong(context, 0);
        if (current >= now)
            return;

        FBUtilities.copyIntoBytes(context, 0, now);
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

    public abstract byte[] update(byte[] context, InetAddress node, long delta);

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

    // swap bytes of step length in context
    protected void swapElement(byte[] context, int left, int right)
    {
        if (left == right) return;

        byte temp;
        for (int i = 0; i < getStepLength(); i++)
        {
            temp = context[left+i];
            context[left+i] = context[right+i];
            context[right+i] = temp;
        }
    }

    // partition bytes of step length in context (for quicksort)
    protected int partitionElements(byte[] context, int left, int right, int pivotIndex)
    {
        int leftOffset  = timestampLength + (left       * getStepLength());
        int rightOffset = timestampLength + (right      * getStepLength());
        int pivotOffset = timestampLength + (pivotIndex * getStepLength());

        byte[] pivotValue = ArrayUtils.subarray(context, pivotOffset, pivotOffset + getStepLength());
        swapElement(context, pivotOffset, rightOffset);
        int storeOffset = leftOffset;
        for (int i = leftOffset; i < rightOffset; i += getStepLength())
        {
            if (FBUtilities.compareByteSubArrays(context, i, pivotValue, 0, getStepLength()) <= 0)
            {
                swapElement(context, i, storeOffset);
                storeOffset += getStepLength();
            }
        }
        swapElement(context, storeOffset, rightOffset);
        return (storeOffset - timestampLength) / getStepLength();
    }

    // quicksort helper
    protected void sortElementsByIdHelper(byte[] context, int left, int right)
    {
        if (right <= left) return;

        int pivotIndex = (left + right) / 2;
        int pivotIndexNew = partitionElements(context, left, right, pivotIndex);
        sortElementsByIdHelper(context, left, pivotIndexNew - 1);
        sortElementsByIdHelper(context, pivotIndexNew + 1, right);
    }

    // quicksort context by id
    protected byte[] sortElementsById(byte[] context)
    {
        assert 0 == ((context.length - timestampLength) % getStepLength()) : "context size is not correct.";
        sortElementsByIdHelper(
            context,
            0,
            (int)((context.length - timestampLength) / getStepLength()) - 1);
        return context;
    }

    protected abstract long tupleCountTotal(byte[] context, int index);

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
                long leftCount  = tupleCountTotal(left,  leftIndex);
                long rightCount = tupleCountTotal(right, rightIndex);

                // advance indexes
                leftIndex  += getStepLength();
                rightIndex += getStepLength();

                // process count comparisons
                if (leftCount == rightCount)
                {
                    continue;
                }
                else if (leftCount > rightCount)
                {
                    switch (relationship)
                    {
                        case EQUAL:
                            relationship = ContextRelationship.GREATER_THAN;
                            break;
                        case GREATER_THAN:
                            continue;
                        default: // LESS_THAN, DISJOINT
                            return ContextRelationship.DISJOINT;
                    }
                }
                else // leftCount < rightCount
                {
                    switch (relationship)
                    {
                        case EQUAL:
                            relationship = ContextRelationship.LESS_THAN;
                            break;
                        case LESS_THAN:
                            continue;
                        default: // GREATER_THAN, DISJOINT
                            return ContextRelationship.DISJOINT;
                    }
                }
            }
            else if (compareId > 0)
            {
                // only advance the right context
                rightIndex += getStepLength();

                switch (relationship)
                {
                    case EQUAL:
                        relationship = ContextRelationship.LESS_THAN;
                        break;
                    case LESS_THAN:
                        continue;
                    default: // GREATER_THAN, DISJOINT
                        return ContextRelationship.DISJOINT;
                }
            }
            else // compareId < 0
            {
                // only advance the left context
                leftIndex += getStepLength();

                switch (relationship)
                {
                    case EQUAL:
                        relationship = ContextRelationship.GREATER_THAN;
                        break;
                    case GREATER_THAN:
                        continue;
                    default: // LESS_THAN, DISJOINT
                        return ContextRelationship.DISJOINT;
                }
            }
        }

        // check final lengths
        if (leftIndex < left.length)
        {
            switch (relationship)
            {
                case EQUAL:
                    return ContextRelationship.GREATER_THAN;
                case LESS_THAN:
                    return ContextRelationship.DISJOINT;
            }
        }
        else if (rightIndex < right.length)
        {
            switch (relationship)
            {
                case EQUAL:
                    return ContextRelationship.LESS_THAN;
                case GREATER_THAN:
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
    public abstract byte[] merge(List<byte[]> contexts);

    /**
     * Human-readable String from context.
     *
     * @param context
     *            version context.
     * @return a human-readable String of the context.
     */
    public abstract String toString(byte[] context);

    // return an aggregated count across all node ids
    public abstract byte[] total(byte[] context);

    // remove the count for a given node id
    public byte[] cleanNodeCounts(byte[] context, InetAddress node)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = timestampLength; offset < context.length; offset += getStepLength())
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: remove node count
            byte[] truncatedContext = new byte[context.length - getStepLength()];
            System.arraycopy(context, 0, truncatedContext, 0, offset);
            System.arraycopy(
                context,
                offset + getStepLength(),
                truncatedContext,
                offset,
                context.length - (offset + getStepLength()));
            return truncatedContext;
        }

        return context;
    }
}
