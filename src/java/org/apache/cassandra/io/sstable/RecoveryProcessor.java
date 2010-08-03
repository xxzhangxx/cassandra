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
package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.ColumnFamily;

/**
 * As a recovery is run over an SSTable the columns may require modification.
 * This interface provides us with a method of doing so.
 */
public interface RecoveryProcessor
{

    /**
     * @param cf modify this column family in a way required by the specific recovery process.
     */
    void process(ColumnFamily cf);

    /**
     * For when we don't need to do anything.
     */
    public static final RecoveryProcessor NO_OP = new RecoveryProcessor()
    {
        
        @Override
        public void process(ColumnFamily cf)
        {
        }
    };

    
    
}
