package com.yahoo.ycsb.tsdb;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.FloatByteIterator;

/**
 * A data point that consists of a time stamp and a float value.
 *
 */
public class DataPoint {
    private final long timestamp;
    private final float value;

    /**
     * Construct a DataPoint with given timestamp and float value.
     * @param ts Timestamp of this data point
     * @param v Float value of this data point
     */
    public DataPoint(long ts, float v) {
        timestamp = ts;
        value = v;
    }

    /**
     * @return The timestamp of this data point
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return The float value of this data point
     */
    public float getValue() {
        return value;
    }

    /**
     * @return Value of this data point as a ByteIterator
     */
    public ByteIterator getValueAsByteIterator() {
        return new FloatByteIterator(value);
    }

    @Override
    public String toString() {
        return String.format("timestamp=%d, value=%f", timestamp, value);
    }
}
