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

import org.apache.cassandra.io.ICompactSerializer2;

/**
 * column family type enum w/ helper methods.
 */
public enum ColumnType
{
    Standard(0),
    Super(ColumnType.SUPER),
    Version(ColumnType.VERSION),
    SuperVersion(ColumnType.SUPER | ColumnType.VERSION),
    IncrementCounter(ColumnType.INCR_COUNT),
    SuperIncrementCounter(ColumnType.SUPER | ColumnType.INCR_COUNT);

    private final static int SUPER      = 1;
    private final static int VERSION    = 1 << 1;
    private final static int INCR_COUNT = 1 << 2;

    private final boolean isSuper;
    private final boolean isContext;
    private final boolean isVersion;
    private final boolean isIncrementCounter;

    public final static ColumnType create(String name)
    {
        return name == null ? Standard : ColumnType.valueOf(name);
    }

    ColumnType(int flags)
    {
        this.isSuper            = (SUPER      & flags) > 0;
        this.isVersion          = (VERSION    & flags) > 0;
        this.isIncrementCounter = (INCR_COUNT & flags) > 0;

        this.isContext = this.isVersion || this.isIncrementCounter;
    }

    public final boolean isSuper()
    {
        return isSuper;
    }

    public final boolean isContext()
    {
        return isContext;
    }

    public final boolean isIncrementCounter()
    {
        return isIncrementCounter;
    }

    public final IClock minClock()
    {
        if (isIncrementCounter)
        {
            return IncrementCounterClock.MIN_VALUE;
        }
        return TimestampClock.MIN_VALUE;
    }

    public final ICompactSerializer2<IClock> clockSerializer()
    {
        if (isIncrementCounter)
        {
            return IncrementCounterClock.SERIALIZER;
        }

        return TimestampClock.SERIALIZER;
    }
}
