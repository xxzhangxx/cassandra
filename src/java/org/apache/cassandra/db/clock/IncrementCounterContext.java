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
public class IncrementCounterContext extends AbstractCounterContext
{
    private static final int countLength = 8; // long

    private static final int stepLength = idLength + countLength;

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final IncrementCounterContext incrementCounterContext = new IncrementCounterContext();
    }

    public static IncrementCounterContext instance()
    {
        return LazyHolder.incrementCounterContext;
    }

    protected int getStepLength()
    {
        return stepLength;
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

    public byte[] update(byte[] context, InetAddress node, long delta)
    {
        updateTimestamp(context);

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
                context,                        // src
                timestampLength,                // srcPos
                context,                        // dest
                timestampLength + stepLength,   // destPos
                offset - timestampLength);      // length
            writeElement(context, nodeId, count + delta);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];

        System.arraycopy(previous, 0, context, 0, timestampLength);
        writeElement(context, nodeId, delta);
        System.arraycopy(
            previous,                           // src
            timestampLength,                    // srcPos
            context,                            // dest
            timestampLength + stepLength,       // destPos
            previous.length - timestampLength); // length

        return context;
    }

    protected long tupleCountTotal(byte[] context, int index)
    {
        return FBUtilities.byteArrayToLong(context, index + idLength);
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
        //      a) local id:  sum counts
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
}