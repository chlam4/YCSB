package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/**
 * Generates floats randomly uniform from an interval.
 */
public class UniformFloatGenerator extends FloatGenerator {
    float _lb, _ub, _interval;

    /**
     * Creates a generator that will return floats uniformly randomly from the
     * interval [lb,ub] inclusive (that is, lb and ub are possible values)
     *
     * @param lb
     *            the lower bound (inclusive) of generated values
     * @param ub
     *            the upper bound (inclusive) of generated values
     */
    public UniformFloatGenerator(float lb, float ub) {
        _lb = lb;
        _ub = ub;
        _interval = _ub - _lb;
    }

    @Override
    public float nextFloat() {
        float ret = Utils.random().nextFloat() * _interval + _lb;
        setLastFloat(ret);

        return ret;
    }

    @Override
    public double mean() {
        return (double) (_lb + _ub) / 2;
    }

}
