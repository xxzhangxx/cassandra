/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

import static org.apache.cassandra.Util.concatByteArrays;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.context.ConcatenatingReconciler;
import org.apache.cassandra.db.context.IncrementCounterReconciler;
import org.apache.cassandra.utils.FBUtilities;

//TODO: REMOVE
import org.apache.commons.lang.ArrayUtils;

public class SuperColumnTest
{   
    @Test
    public void testMissingSubcolumn() {
    	SuperColumn sc = new SuperColumn("sc1".getBytes(), new LongType(), ColumnType.Super, null);
    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new TimestampClock(1)));
    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(2)));
    }

    @Test
    public void testAddColumnVersionVector()
    {
    	SuperColumn sc = new SuperColumn("sc1".getBytes(), new LongType(), ColumnType.SuperVersion, new ConcatenatingReconciler());

    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(2L)
            ))));
    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(3L)
            ))));

    	sc.addColumn(new Column(getBytes(2), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            ))));

    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(3)));
        assert 3 == sc.getObjectCount();

        assert 0 == FBUtilities.compareByteArrays(
            ((VersionVectorClock)sc.getSubColumn(getBytes(1)).clock()).context(),
            concatByteArrays(
                FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L),
                FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L),
                FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L)
                ));

        assert 0 == FBUtilities.compareByteArrays(
            ((VersionVectorClock)sc.getSubColumn(getBytes(2)).clock()).context(),
            concatByteArrays(
                FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L),
                FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
                ));
    }

    @Test
    public void testMostRecentLiveChangeAtVersionVector()
    {
    	SuperColumn sc = new SuperColumn("sc1".getBytes(), new LongType(), ColumnType.SuperVersion, new ConcatenatingReconciler());

    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(2L)
            ))));
    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(3L)
            ))));

    	sc.addColumn(new Column(getBytes(2), "value".getBytes(), new VersionVectorClock(concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L)
            ))));

        assert 0 == FBUtilities.compareByteArrays(
            ((VersionVectorClock)sc.mostRecentLiveChangeAt()).context(),
            concatByteArrays(
                FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L),
                FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L),
                FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L),
                FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L),
                FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L)
                ));
    }

    @Test
    public void testAddColumnIncrementCounter()
    {
    	SuperColumn sc = new SuperColumn("sc1".getBytes(), new LongType(), ColumnType.SuperIncrementCounter, new IncrementCounterReconciler());

    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new IncrementCounterClock(concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(1L)
            ))));
    	sc.addColumn(new Column(getBytes(1), "value".getBytes(), new IncrementCounterClock(concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(10L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(4L)
            ))));

    	sc.addColumn(new Column(getBytes(2), "value".getBytes(), new IncrementCounterClock(concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(8L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(7L)
            ))));

    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNull(sc.getSubColumn(getBytes(3)));
        assert 3 == sc.getObjectCount();

        // column: 1
        assert 0 == FBUtilities.compareByteArrays(
            ((IncrementCounterClock)sc.getSubColumn(getBytes(1)).clock()).context(),
            concatByteArrays(
                FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(12L), FBUtilities.toByteArray(10L),
                FBUtilities.toByteArray(8), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(6L),
                FBUtilities.toByteArray(4), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(5L),
                FBUtilities.toByteArray(1), FBUtilities.toByteArray(7L), FBUtilities.toByteArray(3L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(5L), FBUtilities.toByteArray(2L)
                ));

        // column: 2
        assert 0 == FBUtilities.compareByteArrays(
            ((IncrementCounterClock)sc.getSubColumn(getBytes(2)).clock()).context(),
            concatByteArrays(
                FBUtilities.toByteArray(3), FBUtilities.toByteArray(6L), FBUtilities.toByteArray(9L),
                FBUtilities.toByteArray(7), FBUtilities.toByteArray(3L), FBUtilities.toByteArray(8L),
                FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(7L)
                ));

    	assertNotNull(sc.getSubColumn(getBytes(1)));
    	assertNotNull(sc.getSubColumn(getBytes(2)));
    	assertNull(sc.getSubColumn(getBytes(3)));
    }
}
