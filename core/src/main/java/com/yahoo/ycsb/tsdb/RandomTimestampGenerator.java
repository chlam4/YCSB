package com.yahoo.ycsb.tsdb;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Generator of random time stamps over a given range.
 */
public class RandomTimestampGenerator implements TimestampGenerator {
    private long floor;   // inclusive
    private long ceiling; // inclusive
    private final Random rand = new Random();
    private final ScheduledExecutorService scheduler;

    public RandomTimestampGenerator(final long floor, final long ceiling) {
        this(floor, ceiling, 0, null);
    }

    public RandomTimestampGenerator(final long floor, final long ceiling, final int refreshInterval, final TimeUnit timeUnit) {
        if (floor > ceiling){
            this.floor = ceiling;
            this.ceiling = floor;
        } else {
            this.floor = floor;
            this.ceiling = ceiling;
        }
        if (refreshInterval > 0) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    RandomTimestampGenerator.this.floor = floor + refreshInterval;
                    RandomTimestampGenerator.this.ceiling = ceiling + refreshInterval;
                }}, refreshInterval, refreshInterval, timeUnit
            );
        } else {
            scheduler = null;
        }
    }

    @Override
    public long next() {
        if (floor == ceiling) {
            return floor;
        } else {
            return (rand.nextLong() & ~(1 << (Long.SIZE-1))) % (ceiling + 1 - floor) + floor;
        }
    }

    @Override
    public void cleanup() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
}
