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

package org.apache.cassandra.db;

import java.util.Arrays;
import java.util.Collection;
import java.security.MessageDigest;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Column is immutable, which prevents all kinds of confusion in a multithreaded environment.
 * (TODO: look at making SuperColumn immutable too.  This is trickier but is probably doable
 *  with something like PCollections -- http://code.google.com
 */

public class Column implements IColumn
{
    private static Logger logger = LoggerFactory.getLogger(Column.class);

    public static ColumnSerializer serializer(ClockType clockType)
    {
        return new ColumnSerializer(clockType);
    }

    protected final byte[] name;
    protected final byte[] value;
    protected final IClock clock;

    Column(byte[] name)
    {
        this(name, ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    public Column(byte[] name, byte[] value)
    {
        // safe to set to null, only used for filter comparisons
        this(name, value, null);
    }

    public Column(byte[] name, byte[] value, IClock clock)
    {
        assert name != null;
        assert value != null;
        assert name.length <= IColumn.MAX_NAME_LENGTH;
        this.name = name;
        this.value = value;
        this.clock = clock;
    }

    public byte[] name()
    {
        return name;
    }

    public Column getSubColumn(byte[] columnName)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public byte[] value()
    {
        return value;
    }

    public Collection<IColumn> getSubColumns()
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public IClock clock()
    {
        return clock;
    }

    public boolean isMarkedForDelete()
    {
        return false;
    }

    public IClock getMarkedForDeleteAt()
    {
        throw new IllegalStateException("column is not marked for delete");
    }

    public IClock mostRecentLiveChangeAt()
    {
        return clock;
    }

    public int size()
    {
        /*
         * Size of a column is =
         *   size of a name (short + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + x bytes depending on IClock size
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */
        return DBConstants.shortSize_ + name.length + DBConstants.boolSize_ + clock.size() + DBConstants.intSize_ + value.length;
    }

    /*
     * This returns the size of the column when serialized.
     * @see com.facebook.infrastructure.db.IColumn#serializedSize()
    */
    public int serializedSize()
    {
        return size();
    }

    public void addColumn(IColumn column)
    {
        throw new UnsupportedOperationException("This operation is not supported for simple columns.");
    }

    public IColumn diff(IColumn column)
    {
        return clock.diff(column, this);
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(name);
        digest.update(value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            clock.serialize(buffer);
            buffer.writeByte((isMarkedForDelete()) ? ColumnSerializer.DELETION_MASK : 0);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    public int getLocalDeletionTime()
    {
        throw new IllegalStateException("column is not marked for delete");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Column column = (Column)o;

        if (clock != null ? !clock.equals(column.clock) : column.clock != null)
            return false;
        if (!Arrays.equals(name, column.name))
            return false;

        return Arrays.equals(value, column.value);
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? Arrays.hashCode(name) : 0;
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        result = 31 * result + (clock != null ? clock.hashCode() : 0);
        return result;
    }

    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value.length);
        sb.append("@");
        sb.append(clock.toString());
        return sb.toString();
    }
}

