package com.yahoo.ycsb;

/**
 * A float number's byte iterator.
 */
public class FloatByteIterator extends StringByteIterator {
    /**
     * Construct a FloatByteIterator given a float number
     * @param f the input float number
     */
    public FloatByteIterator(float f) {
        super(Float.toString(f));
    }
}
