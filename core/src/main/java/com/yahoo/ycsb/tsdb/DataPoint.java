package com.yahoo.ycsb.tsdb;

import com.yahoo.ycsb.ByteIterator;

/**
 * A data point that consists of a time stamp and a value represented as
 * an iterator of bytes.
 *
 */
public class DataPoint {
    private final long timestamp;
    private final ByteIterator value;
    
    public DataPoint(long ts, ByteIterator v) {
        timestamp = ts;
        value = v;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public ByteIterator getValue() {
        return value;
    }
}
