package com.yahoo.ycsb.tsdb;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StepTimestampGeneratorTest {

    @DataProvider
    public Object[][] dpWithStep() {
        return new Object[][]{
                {0, 5, 2, new long[]{5, 5, 10, 10, 15, 15, 20}},
                {1447700000L, 1000, 3L, new long[]{1447701000L, 1447701000L, 1447701000L, 1447702000L, 1447702000L, 1447702000L, 1447703000L}}
        };
    }

    @DataProvider
    public Object[][] dpWithoutStep() {
        return new Object[][]{
            {1000, TimeUnit.MILLISECONDS, 10},
            {100, TimeUnit.SECONDS, 3},
            {10000, TimeUnit.NANOSECONDS, 5}
        };
    }

    @Test(dataProvider = "dpWithStep")
    public void testWithStep(final long startTime, final int step,
            final long percount, final long[] generatedTimestamps) {
        final TimestampGenerator tsgen = new StepTimestampGenerator(startTime, step, percount);
        for (final long ts : generatedTimestamps) {
            Assert.assertEquals(tsgen.next(), ts);
        }
    }

    @Test(dataProvider = "dpWithoutStep")
    public void testWithoutStep(final long percount, final TimeUnit timeUnit, final int testCycles) {
        final TimestampGenerator tsgen = new StepTimestampGenerator(percount, timeUnit);
        final long skew = java.lang.Math.max(1,  timeUnit.convert(2, TimeUnit.MILLISECONDS));
        for (int i=0; i<testCycles; i++) {
            final long currTime = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            for (int j=0; j<percount; j++) {
                final long ts = tsgen.next();
                Assert.assertTrue(java.lang.Math.abs(ts-currTime) < skew,
                        String.format("Current time: %d, Generated timestamp: %d, skew: %d", currTime, ts, skew));
            }
        }
    }
}
