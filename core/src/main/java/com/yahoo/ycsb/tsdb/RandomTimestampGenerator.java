package com.yahoo.ycsb.tsdb;

import java.util.Random;

/**
 * Generator of random time stamps over a given range.
 */
public class RandomTimestampGenerator implements TimestampGenerator {
    private final Random rand = new Random();
    private final long floor;   // inclusive
    private final long ceiling; // inclusive

    public RandomTimestampGenerator(long floor, long ceiling) {
        if (floor > ceiling){
            this.floor = ceiling;
            this.ceiling = floor;
        } else {
            this.floor = floor;
            this.ceiling = ceiling;
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
}
