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
package org.apache.cassandra.db.context;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.VersionVectorClock;
import org.apache.cassandra.utils.FBUtilities;

public class ConcatenatingReconcilerTest
{   
    private static final ConcatenatingReconciler reconciler = new ConcatenatingReconciler();

    @Test
    public void testReconcileNormal()
    {
        VersionVectorClock leftClock;
        VersionVectorClock rightClock;

        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // normal + normal
        leftClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32)
            ));
        left = new Column(
            "x".getBytes(),
            FBUtilities.toByteArray(123),
            leftClock,
            false
            );

        rightClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(4),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2)
            ));
        right = new Column(
            "x".getBytes(),
            FBUtilities.toByteArray(456),
            rightClock,
            false
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((VersionVectorClock)leftClock.getSuperset(clocks)).context(),
            ((VersionVectorClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            Util.concatByteArrays(FBUtilities.toByteArray(123), FBUtilities.toByteArray(456)),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;
    }

    @Test
    public void testReconcileMixed()
    {
        // note: merge clocks + only take valid column's value
        VersionVectorClock leftClock;
        VersionVectorClock rightClock;

        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // normal + delete
        leftClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32)
            ));
        left = new Column(
            "x".getBytes(),
            FBUtilities.toByteArray(123),
            leftClock,
            false
            );

        rightClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(4),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2)
            ));
        right = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(124).array(), // localDeleteTime secs
            rightClock,
            true
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((VersionVectorClock)leftClock.getSuperset(clocks)).context(),
            ((VersionVectorClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            FBUtilities.toByteArray(123),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;

        // delete + normal
        leftClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32)
            ));
        left = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(139).array(), // localDeleteTime secs
            leftClock,
            true
            );

        rightClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(4),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2)
            ));
        right = new Column(
            "x".getBytes(),
            FBUtilities.toByteArray(456),
            rightClock,
            false
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((VersionVectorClock)leftClock.getSuperset(clocks)).context(),
            ((VersionVectorClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            FBUtilities.toByteArray(456),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;
    }

    @Test
    public void testReconcileDeleted()
    {
        // note: merge clocks + take later localDeleteTime
        VersionVectorClock leftClock;
        VersionVectorClock rightClock;

        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // delete + delete
        leftClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32)
            ));
        left = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(139).array(), // localDeleteTime secs
            leftClock,
            true
            );

        rightClock = new VersionVectorClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(4),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2)
            ));
        right = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(124).array(), // localDeleteTime secs
            rightClock,
            true
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((VersionVectorClock)leftClock.getSuperset(clocks)).context(),
            ((VersionVectorClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            FBUtilities.toByteArray(139),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == true;
    }
}
