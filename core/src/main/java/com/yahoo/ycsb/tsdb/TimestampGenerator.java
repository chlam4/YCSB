package com.yahoo.ycsb.tsdb;

/**
 * An interface for generating a sequence of time stamps for testing time
 * series databases.
 */
public interface TimestampGenerator {
    /**
     * Return the next time stamp in the sequence.
     * @return The next time stamp in nanoseconds since epoch.
     */
    public long nextTimestamp();
}
