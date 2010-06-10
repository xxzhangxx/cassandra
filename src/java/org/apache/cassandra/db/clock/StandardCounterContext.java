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
 * An implementation of a distributed counter context.
 *
 * The context format is:
 *   {timestamp + [(node id, incr count, decr count), ...]}
 *
 * On update:
 *   1) update timestamp to: max(timestamp, local time), and
 *   2) node updating value will increment the associated count.
 *
 * The aggregated count can be calculated by rolling up all the counts from each
 * (node id, incr count, decr count) pair.
 */
public class StandardCounterContext extends AbstractCounterContext
{
    private static final int incrCountLength = 8; // long
    private static final int decrCountLength = 8; // long

    private static final int stepLength = idLength + incrCountLength + decrCountLength;

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final StandardCounterContext standardCounterContext = new StandardCounterContext();
    }

    public static StandardCounterContext instance()
    {
        return LazyHolder.standardCounterContext;
    }

    protected int getStepLength()
    {
        return stepLength;
    }

    // write a tuple (node id, incr count, decr count) at the front
    protected void writeElement(byte[] context, byte[] id_, long incrCount, long decrCount)
    {
        writeElementAtStepOffset(context, 0, id_, incrCount, decrCount);
    }

    // write a tuple (node id, incr count, decr count) at step offset
    protected void writeElementAtStepOffset(byte[] context, int stepOffset, byte[] id_, long incrCount, long decrCount)
    {
        int offset = timestampLength + (stepOffset * stepLength);
        System.arraycopy(id_, 0, context, offset, idLength);
        FBUtilities.copyIntoBytes(context, offset + idLength, incrCount);
        FBUtilities.copyIntoBytes(context, offset + idLength + incrCountLength, decrCount);
    }

    public byte[] update(byte[] context, InetAddress node, long delta)
    {
        updateTimestamp(context);

        // calculate node id
        byte[] nodeId = node.getAddress();

        // calculate deltas
        long incrDelta = delta > 0 ?  delta : 0;
        long decrDelta = delta < 0 ? -delta : 0;

        // look for this node id
        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: increment count, shift to front
            long incrCount = FBUtilities.byteArrayToLong(context, offset + idLength);
            long decrCount = FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength);

            System.arraycopy(
                context,                        // src
                timestampLength,                // srcPos
                context,                        // dest
                timestampLength + stepLength,   // destPos
                offset - timestampLength);      // length
            writeElement(context, nodeId, incrCount + incrDelta, decrCount + decrDelta);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];

        System.arraycopy(previous, 0, context, 0, timestampLength);
        writeElement(context, nodeId, incrDelta, decrDelta);
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
        return FBUtilities.byteArrayToLong(context, index + idLength) + 
               FBUtilities.byteArrayToLong(context, index + idLength + incrCountLength);
    }

    private class StandardCounterNode
    {
        public long incrCount;
        public long decrCount;

        public StandardCounterNode(long incrCount, long decrCount)
        {
            this.incrCount = incrCount;
            this.decrCount = decrCount;
        }

        public int compareTo(StandardCounterNode o)
        {
            long left  = incrCount   + decrCount;
            long right = o.incrCount + o.decrCount;
            if (left == right)
            {
                return 0;
            }
            else if (left > right)
            {
                return 1;
            }
            // left < right
            return -1;
        }

        @Override
        public String toString()
        {
            return "(" + incrCount + "," + decrCount + ")";
        }
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
        //   2) map id -> (incr count, decr count) tuples
        //      a) local id:  sum incr and decr counts, respectively
        //      b) remote id: keep highest incr and decr counts (reconcile)
        //   3) create a context from sorted array
        long highestTimestamp = Long.MIN_VALUE;
        Map<FBUtilities.ByteArrayWrapper, StandardCounterNode> contextsMap =
            new HashMap<FBUtilities.ByteArrayWrapper, StandardCounterNode>();
        for (byte[] context : contexts)
        {
            // take highest timestamp
            highestTimestamp = Math.max(FBUtilities.byteArrayToLong(context, 0), highestTimestamp);

            // map id -> (incr count, decr count) tuple
            for (int offset = timestampLength; offset < context.length; offset += stepLength)
            {
                FBUtilities.ByteArrayWrapper id = new FBUtilities.ByteArrayWrapper(
                        ArrayUtils.subarray(context, offset, offset + idLength));
                long incrCount = FBUtilities.byteArrayToLong(context, offset + idLength);
                long decrCount = FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength);

                if (!contextsMap.containsKey(id))
                {
                    contextsMap.put(id, new StandardCounterNode(incrCount, decrCount));
                    continue;
                }

                StandardCounterNode node = contextsMap.get(id);

                // local id: sum respective counts
                if (this.idWrapper.equals(id))
                {
                    node.incrCount += incrCount;
                    node.decrCount += decrCount;
                    continue;
                }

                // remote id: keep highest set of counts
                if ((node.incrCount + node.decrCount) < (incrCount + decrCount))
                {
                    node.incrCount = incrCount;
                    node.decrCount = decrCount;
                }
            }
        }

        List<Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode>> contextsList =
            new ArrayList<Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode>>(
                    contextsMap.entrySet());
        Collections.sort(
            contextsList,
            new Comparator<Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode>>()
            {
                public int compare(
                    Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode> e1,
                    Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode> e2)
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
            Map.Entry<FBUtilities.ByteArrayWrapper, StandardCounterNode> entry = contextsList.get(i);
            writeElementAtStepOffset(
                merged,
                i,
                entry.getKey().data,
                entry.getValue().incrCount,
                entry.getValue().decrCount);
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
            sb.append(", ");
            sb.append(FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength));
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
            long incrCount = FBUtilities.byteArrayToLong(context, offset + idLength);
            long decrCount = FBUtilities.byteArrayToLong(context, offset + idLength + incrCountLength);
            total += incrCount - decrCount;
        }

        return FBUtilities.toByteArray(total);
    }
}
