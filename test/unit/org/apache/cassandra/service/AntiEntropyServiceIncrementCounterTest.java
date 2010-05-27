package org.apache.cassandra.service;

import org.apache.cassandra.db.IncrementCounterClock;
import org.junit.Before;

public class AntiEntropyServiceIncrementCounterTest extends AntiEntropyServiceTestAbstract
{

    @Before
    public void init()
    {
        tablename = "Keyspace4";
        cfname = "IncrementCounter1";
        clock = new IncrementCounterClock(new byte[] {});
    }
    
}