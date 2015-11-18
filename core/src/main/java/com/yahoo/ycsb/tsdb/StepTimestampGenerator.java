package com.yahoo.ycsb.tsdb;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator of a sequence of time stamps in steps.  Each step consists of a
 * given fixed number of time stamps.
 *
 * For example, if start time = 1440000000s, step = 10s, perStepCount = 5,
 * then the sequence of time stamps to be generated is as follows:
 * 1440000000
 * 1440000000
 * 1440000000
 * 1440000000
 * 1440000000
 * 1440000010
 * 1440000010
 * 1440000010
 * 1440000010
 * 1440000010
 * 1440000020
 * ...
 * 
 */
public class StepTimestampGenerator implements TimestampGenerator {
    private final int step;
    private final long perStepCount;
    private final AtomicLong currTimestamp = new AtomicLong();
    private final AtomicLong currCount = new AtomicLong();

    /**
     * Construct a RegularTimestampGenerator given the following parameters
     * @param startTime Start time stamp
     * @param delta Delta to increase from previous time stamp
     * @param countPerTimestamp How many same time stamps to generate before increasing
     */
    public StepTimestampGenerator(final long startTime, final int delta, final long countPerTimestamp) {
        this.step = delta;
        this.perStepCount = countPerTimestamp;
        currTimestamp.set(startTime);
    }

    @Override
    public long next() {
        return (currCount.incrementAndGet() % perStepCount == 0) ? currTimestamp.getAndAdd(step) : currTimestamp.longValue();
    }

}
