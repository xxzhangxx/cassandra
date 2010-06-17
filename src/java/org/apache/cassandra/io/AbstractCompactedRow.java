package org.apache.cassandra.io;

import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.db.DecoratedKey;

/**
 * a CompactedRow is an object that takes a bunch of rows (keys + columnfamilies)
 * and can write a compacted version of those rows to an output stream.  It does
 * NOT necessarily require creating a merged CF object in memory.
 */
public abstract class AbstractCompactedRow
{
    public final DecoratedKey key;
    protected final CompactionIterator compactionIterator;

    public AbstractCompactedRow(DecoratedKey key, CompactionIterator compactionIterator)
    {
        this.key = key;
        this.compactionIterator = compactionIterator;
    }

    public abstract void write(DataOutput out) throws IOException;
    
    public abstract void update(MessageDigest digest);

    public abstract boolean isEmpty();
}
