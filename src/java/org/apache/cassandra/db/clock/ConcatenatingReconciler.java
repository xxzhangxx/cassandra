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

import java.util.List;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.utils.FBUtilities;

public class ConcatenatingReconciler extends AbstractReconciler
{
    private static VersionVectorContext contextManager;

    static
    {
        contextManager = VersionVectorContext.instance();
    }

    public Column reconcile(Column left, Column right)
    {
        // reconciles DISJOINT columns
        // strategy:
        //   - live + live:       concatenate values
        //   - live + deleted:    live value
        //   - deleted + deleted: delete w/ higher local delete timestamp

        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(right.clock());
        IClock mergedClock = (IClock)left.clock().getSuperset(clocks);

        boolean leftDeleted  = left.isMarkedForDelete();
        boolean rightDeleted = right.isMarkedForDelete();
        if (leftDeleted)
        {
            if (rightDeleted)
            {
                // use later local delete time
                int leftLocalDeleteTime  = FBUtilities.byteArrayToInt(left.value());
                int rightLocalDeleteTime = FBUtilities.byteArrayToInt(right.value());

                // both DISJOINT, so both must be marked for delete
                return new DeletedColumn(
                    left.name(),
                    leftLocalDeleteTime >= rightLocalDeleteTime ? left.value() : right.value(),
                    mergedClock);
            }
            else
            {
                return new Column(right.name(), right.value(), mergedClock);
            }
        }
        else if (rightDeleted)
        {
            return new Column(left.name(), left.value(), mergedClock);
        }
        else
        {
            return new Column(
                    left.name(),
                    ArrayUtils.addAll(left.value(), right.value()),
                    mergedClock);
        }
    }
}
