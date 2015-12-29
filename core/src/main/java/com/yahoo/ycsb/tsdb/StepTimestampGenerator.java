package com.yahoo.ycsb.tsdb;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator of a sequence of timestamps in steps.  Each step consists of a
 * given fixed number of timestamps.
 *
 * If a start time is given, then it will be used as the first timestamp.  If
 * not, then the current system time is used.
 *
 * If a step size is given, then it will be used to increment the timestamp.
 * If not, then the next timestamp will be generated based on the real system
 * time.
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

    //
    // Size of each timestamp increment; if less than 1, then the next
    // timestamp be generated based on the system time.
    //
    private final int step;

    //
    // Number of timestamps that would be generated in each step
    //
    private final long perStepCount;

    //
    // The next timestamp to be returned from this generator
    //
    private final AtomicLong currTimestamp = new AtomicLong();

    //
    // Number of current timestamps that have been returned/generated
    //
    private final AtomicLong currCount = new AtomicLong();

    //
    // The time unit of the timestamps to be generated by this generator; this
    // is used when either start time or step size is specified to generate the
    // next timestamp based on the system time.
    //
    private final TimeUnit timeUnit;

    /**
     * Construct a StepTimestampGenerator given perStepCount and timeUnit.
     *
     * Since the startTime is not specified, it is initialized using the
     * current system time.
     *
     * The step size is also not specified, so it is set to 0 and the next
     * timestamp is generated using the system time upon generation.
     *
     * @param perStepCount Number of timestamps to be generated in each step
     * @param timeUnit Time unit of the timestamps to be generated by this generator
     */
    public StepTimestampGenerator(final long perStepCount, final TimeUnit timeUnit) {
        this(getCurrTimestamp(timeUnit), 0, perStepCount, timeUnit);
    }

    /**
     * Construct a StepTimestampGenerator given the following parameters.
     * @param startTime Start time stamp
     * @param delta Delta to increase from previous time stamp
     * @param perStepCount Number of timestamps to be generated in each step
     */
    public StepTimestampGenerator(final long startTime, final int delta, final long perStepCount) {
        this(startTime, delta, perStepCount, null);
    }

    /**
     * Private constructor that initializes all the variables properly.
     * @param startTime
     * @param delta
     * @param perStepCount
     * @param timeUnit
     */
    private StepTimestampGenerator(final long startTime, final int delta, final long perStepCount, final TimeUnit timeUnit) {
        if (delta < 1 && timeUnit == null) {
            throw new IllegalArgumentException(String.format(
                    "Specified timestamp increment %d is less than one (unit) while no time unit is specified to generate the next timestamp.", delta));
        }
        this.step = delta;
        this.perStepCount = perStepCount;
        this.timeUnit = timeUnit;
        currTimestamp.set(startTime);
    }

    /**
     * Return the current system time in the specified time unit.
     * If the time unit is not specified, then current system time in
     * milliseconds is returned.
     */
    private static long getCurrTimestamp(TimeUnit timeUnit) {
        if (timeUnit == null || timeUnit.equals(TimeUnit.MILLISECONDS)) {
            return System.currentTimeMillis();
        } else {
            return timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public long next() {
        if (currCount.getAndIncrement() % perStepCount == 0) {
            if (step >= 1) {
                currTimestamp.addAndGet(step);
            } else {
                currTimestamp.set(getCurrTimestamp(timeUnit));
            }
        }
        return currTimestamp.longValue();
    }

    @Override
    public void cleanup() {
        // nothing needs to be cleaned up
    }

}