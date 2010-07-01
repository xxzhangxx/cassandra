package org.apache.cassandra.service;

import org.apache.cassandra.db.IncrementCounterClock;

public class AntiEntropyServiceIncrementCounterTest extends AntiEntropyServiceTestAbstract
{

    public void init()
    {
        tablename = "Keyspace4";
        cfname = "IncrementCounter1";
        clock = new IncrementCounterClock();
    }
    
}
