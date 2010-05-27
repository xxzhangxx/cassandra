package org.apache.cassandra.service;

import org.apache.cassandra.db.TimestampClock;
import org.junit.Before;

public class AntiEntropyServiceStandardTest extends AntiEntropyServiceTestAbstract
{

    @Before
    public void init()
    {
        tablename = "Keyspace4";
        cfname = "Standard1";
        clock = new TimestampClock(0);
    }
    
}
