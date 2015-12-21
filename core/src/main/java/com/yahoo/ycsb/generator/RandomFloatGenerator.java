package com.yahoo.ycsb.generator;

import java.util.Random;

/**
 * This generator generates float numbers randomly over a range.
 */
public class RandomFloatGenerator extends FloatGenerator {
    private static final Random rand = new Random();
    private final float lower;    // lower bound of the range - inclusive
    private final float upper;    // upper bound of the range - exclusive

    public RandomFloatGenerator(final float lower, final float upper) {
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public float nextFloat() {
        return rand.nextFloat() * (upper - lower) + lower;
    }

    @Override
    public double mean() {
        return (lower + upper) / 2;
    }

}
