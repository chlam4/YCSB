package com.yahoo.ycsb.tsdb;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generator of a sequence of time stamps that are approximately the real
 * system time at the time of generation.  The returned time stamp is
 * constructed as a sum of two parts: the current system time and an offset.
 * The offset is incremented each time such that the next time stamp is
 * different than the current one.  The increment is based on the configured
 * TimeUnit.
 *
 * The offset is reset to zero when the current system time advances.  This way
 * we keep the generated time stamp close to the real time, which could be
 * essential in testing out the effect of compaction that is usually related
 * to the real time.
 *
 * This aims to make every time stamp distinct, while keeping each time stamp
 * close to the system time when it is generated.  However, if the TimeUnit is
 * too coarse and the generation rate is too high, then there will be
 * overlapping time stamps.
 */
public class ApproxRealTimestampGenerator implements TimestampGenerator {
    private final TimeUnit timeUnit;
    private long baseTime;
    private final AtomicInteger offset;

    public ApproxRealTimestampGenerator(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        baseTime = 0;
        offset = new AtomicInteger();
    }

    @Override
    public long next() {
        long currTime = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        synchronized(this) {
            if (currTime > baseTime) {
                baseTime = currTime;
                offset.set(0);
            }
        }
        return baseTime + offset.getAndIncrement();
    }

    @Override
    public void cleanup() {
        // nothing needs to be cleaned up
    }

}
