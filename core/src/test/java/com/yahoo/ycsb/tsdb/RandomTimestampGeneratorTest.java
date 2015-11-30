package com.yahoo.ycsb.tsdb;

import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.Test;

public class RandomTimestampGeneratorTest {

    @Test
    public void next() {
        final Random rand = new Random();
        for (int i=0; i<1000; i++) {
            final long floor = rand.nextLong() & ~(1 << (Long.SIZE-1));
            final long ceiling = rand.nextLong() & ~(1 << (Long.SIZE-1));
            final RandomTimestampGenerator tsgen = new RandomTimestampGenerator(floor, ceiling);
            for (int j=0; j<10000; j++) {
                final long ts = tsgen.next();
                //System.out.println(String.format("floor=%d, ceiling=%d, generated=%d", floor > ceiling ? ceiling : floor, floor > ceiling ? ceiling : ceiling, ts));
                Assert.assertTrue((ts >= floor && ts < ceiling) || (ts >= ceiling && ts < floor));
            }
        }
    }
}
