package com.yahoo.ycsb.tsdb;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generator of a sequence of distinct time stamps.  The next time stamp
 * returned is guaranteed to be different than any previously generated
 * time stamp.  Each generated time stamp will also be very close to the
 * System.currentTimeMillis().
 */
public class UniqueTimestampGenerator implements TimestampGenerator {
    private long lastTimeMillis = 0;
    private final AtomicInteger nanoOffset = new AtomicInteger();

    @Override
    public long nextTimestamp() {
        long currTimeMillis = System.currentTimeMillis();
        synchronized(UniqueTimestampGenerator.class) {
            if (currTimeMillis > lastTimeMillis) {
                lastTimeMillis = currTimeMillis;
                nanoOffset.set(0);
            }
        }
        return lastTimeMillis * 1000000 + nanoOffset.getAndIncrement();
    }

}
