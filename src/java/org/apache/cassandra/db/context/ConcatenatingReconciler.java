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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.Column;
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
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(right.clock());
        IClock clock = (IClock)left.clock().getSuperset(clocks);

        // only called if DISJOINT, so both must be marked for delete
        if (left.isMarkedForDelete() && right.isMarkedForDelete())
        {
            // use later local delete time
            int leftLocalDeleteTime  = FBUtilities.byteArrayToInt(left.value());
            int rightLocalDeleteTime = FBUtilities.byteArrayToInt(right.value());

            return new Column(
                    left.name(),
                    leftLocalDeleteTime >= rightLocalDeleteTime ? left.value() : right.value(),
                    clock,
                    true);
        }

        byte[] value;
        if (left.isMarkedForDelete())
        {
            value = right.value();
        }
        else if (right.isMarkedForDelete())
        {
            value = left.value();
        }
        else
        {
            value = ArrayUtils.addAll(left.value(), right.value());
        }

        return new Column(
                left.name(),
                value,
                clock,
                false);
    }
}
