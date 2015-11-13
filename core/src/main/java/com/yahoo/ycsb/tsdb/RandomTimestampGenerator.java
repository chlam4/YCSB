package com.yahoo.ycsb.tsdb;

import java.util.Random;

/**
 * Generator of random time stamps over a given range.
 */
public class RandomTimestampGenerator implements TimestampGenerator {
    private static final Random rand = new Random();
    private final long floor;   // inclusive
    private final long ceiling; // exclusive

    public RandomTimestampGenerator(long floor, long ceiling) {
        if (floor == ceiling) {
            this.floor = floor;
            this.ceiling = floor + 1;   // to avoid DivideByZeroError
        } else if (floor > ceiling){
            this.floor = ceiling;
            this.ceiling = floor;
        } else {
            this.floor = floor;
            this.ceiling = ceiling;
        }
    }

    @Override
    public long next() {
        return rand.nextLong() % (ceiling - floor) + floor;
    }
}
