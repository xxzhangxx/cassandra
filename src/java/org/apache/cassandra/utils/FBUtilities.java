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

package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Charsets;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.ArrayUtils;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class FBUtilities
{
    private static Logger logger_ = LoggerFactory.getLogger(FBUtilities.class);

    public static final BigInteger TWO = new BigInteger("2");

    private static volatile InetAddress localInetAddress_;

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

    public static final Comparator<byte[]> byteArrayComparator = new Comparator<byte[]>()
    {
        public int compare(byte[] o1, byte[] o2)
        {
            return compareByteArrays(o1, o2);
        }
    };

    /**
     * Parses a string representing either a fraction, absolute value or percentage.
     */
    public static double parseDoubleOrPercent(String value)
    {
        if (value.endsWith("%"))
        {
            return Double.valueOf(value.substring(0, value.length() - 1)) / 100;
        }
        else
        {
            return Double.valueOf(value);
        }
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress_ == null)
            try
            {
                localInetAddress_ = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress_;
    }

    /**
     * @param fractOrAbs A double that may represent a fraction or absolute value.
     * @param total If fractionOrAbs is a fraction, the total to take the fraction from
     * @return An absolute value which may be larger than the total.
     */
    public static long absoluteFromFraction(double fractOrAbs, long total)
    {
        if (fractOrAbs < 0)
            throw new UnsupportedOperationException("unexpected negative value " + fractOrAbs);

        if (0 < fractOrAbs && fractOrAbs <= 1)
        {
            // fraction
            return Math.max(1, (long)(fractOrAbs * total));
        }

        // absolute
        assert fractOrAbs >= 1 || fractOrAbs == 0;
        return (long)fractOrAbs;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger,Boolean> midpoint(BigInteger left, BigInteger right, int sigbits)
    {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0)
        {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        }
        else
        {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return new Pair(midpoint, remainder);
    }

    /**
     * Copy bytes from int into bytes starting from offset.
     * @param bytes Target array
     * @param offset Offset into the array
     * @param i Value to write
     */
    public static void copyIntoBytes(byte[] bytes, int offset, int i)
    {
        bytes[offset]   = (byte)( ( i >>> 24 ) & 0xFF );
        bytes[offset+1] = (byte)( ( i >>> 16 ) & 0xFF );
        bytes[offset+2] = (byte)( ( i >>> 8  ) & 0xFF );
        bytes[offset+3] = (byte)(   i          & 0xFF );
    }

    /**
     * Copy bytes from long into bytes starting from offset.
     * @param bytes Target array
     * @param offset Offset into the array
     * @param l Value to write
     */
    public static void copyIntoBytes(byte[] bytes, int offset, long l)
    {
        bytes[offset]   = (byte)( ( l >>> 56 ) & 0xFF );
        bytes[offset+1] = (byte)( ( l >>> 48 ) & 0xFF );
        bytes[offset+2] = (byte)( ( l >>> 40 ) & 0xFF );
        bytes[offset+3] = (byte)( ( l >>> 32 ) & 0xFF );
        bytes[offset+4] = (byte)( ( l >>> 24 ) & 0xFF );
        bytes[offset+5] = (byte)( ( l >>> 16 ) & 0xFF );
        bytes[offset+6] = (byte)( ( l >>> 8  ) & 0xFF );
        bytes[offset+7] = (byte)(   l          & 0xFF );
    }

    /**
     * @param i Write this int to an array
     * @return Four byte array containing the int
     */
    public static byte[] toByteArray(int i)
    {
        byte[] bytes = new byte[4];
        copyIntoBytes(bytes, 0, i);
        return bytes;
    }

    /**
     * @param l Write this long to an array
     * @return Four byte array containing the long
     */
    public static byte[] toByteArray(long l)
    {
        byte[] bytes = new byte[8];
        copyIntoBytes(bytes, 0, l);
        return bytes;
    }

    /**
     * @param bytes A byte array containing a serialized integer.
     * @return The integer value contained in the byte array.
     */
    public static int byteArrayToInt(byte[] bytes)
    {
    	return byteArrayToInt(bytes, 0);
    }

    /**
     * @param bytes A byte array containing a serialized integer.
     * @param offset Start position of the integer in the array.
     * @return The integer value contained in the byte array.
     */
    public static int byteArrayToInt(byte[] bytes, int offset)
    {
        if ( bytes.length - offset < 4 )
        {
            throw new IllegalArgumentException("An integer must be 4 bytes in size.");
        }
        int n = 0;
        for ( int i = 0; i < 4; ++i )
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xFF;
        }
        return n;
    }

    /**
     * @param bytes A byte array containing a serialized long.
     * @return The long value contained in the byte array.
     */
    public static long byteArrayToLong(byte[] bytes)
    {
        return byteArrayToLong(bytes, 0);
    }

    /**
     * @param bytes A byte array containing a serialized long.
     * @param offset Start position of the long in the array.
     * @return The long value contained in the byte array.
     */
    public static long byteArrayToLong(byte[] bytes, int offset)
    {
        if ( bytes.length - offset < 8 )
        {
            throw new IllegalArgumentException("A long must be 8 bytes in size.");
        }
        long n = 0;
        for ( int i = 0; i < 8; ++i )
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xFF;
        }
        return n;
    }

    public static int compareByteArrays(byte[] bytes1, byte[] bytes2){
        if(null == bytes1){
            if(null == bytes2) return 0;
            else return -1;
        }
        if(null == bytes2) return 1;

        int minLength = Math.min(bytes1.length, bytes2.length);
        for(int i = 0; i < minLength; i++)
        {
            if(bytes1[i] == bytes2[i])
                continue;
            // compare non-equal bytes as unsigned
            return (bytes1[i] & 0xFF) < (bytes2[i] & 0xFF) ? -1 : 1;
        }
        if(bytes1.length == bytes2.length) return 0;
        else return (bytes1.length < bytes2.length)? -1 : 1;
    }

    /**
     * Compare two byte[] at specified offsets for length. Compares the non equal bytes as unsigned.
     * @param bytes1 First array to compare.
     * @param offset1 Position to start the comparison at in the first array.
     * @param bytes2 Second array to compare.
     * @param offset2 Position to start the comparison at in the second array.
     * @param length How many bytes to compare?
     * @return -1 if byte1 is less than byte2, 1 if byte2 is less than byte1 or 0 if equal.
     */
    public static int compareByteSubArrays(byte[] bytes1, int offset1, byte[] bytes2, int offset2, int length)
    {
        if ( null == bytes1 )
        {
            if ( null == bytes2) return 0;
            else return -1;
        }
        if (null == bytes2 ) return 1;

        assert bytes1.length >= (offset1 + length) : "The first byte array isn't long enough for the specified offset and length.";
        assert bytes2.length >= (offset2 + length) : "The second byte array isn't long enough for the specified offset and length.";
        for ( int i = 0; i < length; i++ )
        {
            byte byte1 = bytes1[offset1+i];
            byte byte2 = bytes2[offset2+i];
            if ( byte1 == byte2 )
                continue;
            // compare non-equal bytes as unsigned
            return (byte1 & 0xFF) < (byte2 & 0xFF) ? -1 : 1;
        }
        return 0;
    }

    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right)
    {
        if (left == null || right == null)
            return null;
        if (left.length > right.length)
        {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
        {
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static BigInteger md5hash(byte[] data)
    {
        byte[] result = hash("MD5", data);
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }

    public static String hexHash(String type, byte[]... data)
    {
        return bytesToHex(hash(type, data));
    }

    public static byte[] hash(String type, byte[]... data)
    {
    	byte[] result;
    	try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            for(byte[] block : data)
                messageDigest.update(block);
            result = messageDigest.digest();
    	}
    	catch (Exception e)
        {
            throw new RuntimeException(e);
    	}
    	return result;
	}

    public static void writeByteArray(byte[] bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] readByteArray(DataInput in) throws IOException
    {
        int length = in.readInt();
        if (length < 0)
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }
        byte[] value = new byte[length];
        if (length > 0)
        {
            in.readFully(value);
        }
        return value;
    }

    public static void writeShortByteArray(byte[] name, DataOutput out)
    {
        int length = name.length;
        assert 0 <= length && length <= MAX_UNSIGNED_SHORT;
        try
        {
            out.writeByte((length >> 8) & 0xFF);
            out.writeByte(length & 0xFF);
            out.write(name);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static byte[] readShortByteArray(DataInput in) throws IOException
    {
        int length = 0;
        length |= (in.readByte() & 0xFF) << 8;
        length |= in.readByte() & 0xFF;
        if (!(0 <= length && length <= MAX_UNSIGNED_SHORT))
            throw new IOException("Corrupt name length " + length);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    public static byte[] hexToBytes(String str)
    {
        assert str.length() % 2 == 0;
        byte[] bytes = new byte[str.length()/2];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte)Integer.parseInt(str.substring(i*2, i*2+2), 16);
        }
        return bytes;
    }

    public static String bytesToHex(byte... bytes)
    {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
        {
            int bint = b & 0xff;
            if (bint <= 0xF)
                // toHexString does not 0 pad its results.
                sb.append("0");
            sb.append(Integer.toHexString(bint));
        }
        return sb.toString();
    }

    public static void renameWithConfirm(String tmpFilename, String filename) throws IOException
    {
        if (!new File(tmpFilename).renameTo(new File(filename)))
        {
            throw new IOException("rename failed of " + filename);
        }
    }

    public static <T extends Comparable<T>> CollatingIterator getCollatingIterator()
    {
        // CollatingIterator will happily NPE if you do not specify a comparator explicitly
        return new CollatingIterator(new Comparator<T>()
        {
            public int compare(T o1, T o2)
            {
                return o1.compareTo(o2);
            }
        });
    }

    public static void atomicSetMax(AtomicInteger atomic, int i)
    {
        while (true)
        {
            int j = atomic.get();
            if (j >= i || atomic.compareAndSet(j, i))
                break;
        }
    }

    public static void atomicSetMax(AtomicLong atomic, long i)
    {
        while (true)
        {
            long j = atomic.get();
            if (j >= i || atomic.compareAndSet(j, i))
                break;
        }
    }

    /** 
     * Sets an atomic clock reference to the maximum of its current value and
     * a new value.
     *
     * The function is not synchronized and does not guarantee that the resulting
     * reference will hold either the old or new value, but it does guarantee
     * that it will hold a value, v, such that: v = max(oldValue, newValue, v).
     *
     * @param atomic the atomic reference to set
     * @param newClock the new provided value
     */
    public static void atomicSetMax(AtomicReference<IClock> atomic, IClock newClock)
    {
        while (true)
        {
            IClock oldClock = atomic.get();
            ClockRelationship rel = oldClock.compare(newClock);
            if (rel == ClockRelationship.DISJOINT)
            {
                newClock = oldClock.getSuperset(Arrays.asList(newClock));
            }
            if (rel == ClockRelationship.GREATER_THAN || rel == ClockRelationship.EQUAL 
                || atomic.compareAndSet(oldClock, newClock))
                break;
        }
    }

    public static void serialize(TSerializer serializer, TBase struct, DataOutput out)
    throws IOException
    {
        assert serializer != null;
        assert struct != null;
        assert out != null;
        byte[] bytes;
        try
        {
            bytes = serializer.serialize(struct);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void deserialize(TDeserializer deserializer, TBase struct, DataInput in)
    throws IOException
    {
        assert deserializer != null;
        assert struct != null;
        assert in != null;
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        try
        {
            deserializer.deserialize(struct, bytes);
        }
        catch (TException ex)
        {
            throw new IOException(ex);
        }
    }

    public static void sortSampledKeys(List<DecoratedKey> keys, Range range)
    {
        if (range.left.compareTo(range.right) >= 0)
        {
            // range wraps.  have to be careful that we sort in the same order as the range to find the right midpoint.
            final Token right = range.right;
            Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
            {
                public int compare(DecoratedKey o1, DecoratedKey o2)
                {
                    if ((right.compareTo(o1.token) < 0 && right.compareTo(o2.token) < 0)
                        || (right.compareTo(o1.token) > 0 && right.compareTo(o2.token) > 0))
                    {
                        // both tokens are on the same side of the wrap point
                        return o1.compareTo(o2);
                    }
                    return -o1.compareTo(o2);
                }
            };
            Collections.sort(keys, comparator);
        }
        else
        {
            // unwrapped range (left < right).  standard sort is all we need.
            Collections.sort(keys);
        }
    }

    public static int encodedUTF8Length(String st)
    {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++)
        {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }

    public static String decodeToUTF8(byte[] bytes) throws CharacterCodingException
    {
        return Charsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Test if a particular bit is set using a bit mask.
     *
     * @param v the value in which a bit must be tested
     * @param mask the bit mask use to select a bit of <code>v</code>
     * @return true if the bit of <code>v</code> selected by <code>mask<code>
     * is set, false otherwise.
     */
    public static boolean testBitUsingBitMask(int v, int mask)
    {
        return (v & mask) != 0;
    }

    /** 
     * Thin wrapper around byte[] to provide meaningful equals() and hashCode() operations
     * caveat: assumed that wrapped byte[] will not be modified
     */
    public static final class ByteArrayWrapper
    {
        public final byte[] data;

        public ByteArrayWrapper(byte[] data)
        {
            if ( null == data )
            {
                throw new NullPointerException();
            }
            this.data = data;
        }

        public boolean equals(Object other)
        {
            if ( !( other instanceof ByteArrayWrapper ) )
            {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper)other).data);
        }

        public int hashCode()
        {
            return Arrays.hashCode(data);
        }

        public String toString()
        {
            return ArrayUtils.toString(data);
        }
    }

    public static String resourceToFile(String filename) throws ConfigurationException
    {
        ClassLoader loader = PropertyFileSnitch.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return scpurl.getFile();
    }
}
