package com.yahoo.ycsb.generator;

/**
 * This float generator always returns the same float number.
 */
public class FixedFloatGenerator extends FloatGenerator {
    private final float f;

    public FixedFloatGenerator(float f) {
        this.f = f;
    }

    @Override
    public float nextFloat() {
        return f;
    }

    @Override
    public double mean() {
        return f;
    }

}
