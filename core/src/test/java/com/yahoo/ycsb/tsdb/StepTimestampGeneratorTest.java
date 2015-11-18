package com.yahoo.ycsb.tsdb;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StepTimestampGeneratorTest {

    @DataProvider
    public Object[][] nextdp() {
        return new Object[][]{
                {0, 5, 2, new long[]{0, 0, 5, 5, 10, 10, 15}},
                {1447700000L, 1000, 3L, new long[]{1447700000L, 1447700000L, 1447700000L, 1447701000L, 1447701000L, 1447701000L, 1447702000L}}
        };
    }

    @Test(dataProvider = "nextdp")
    public void next(final long startTime, final int step,
            final long percount, final long[] generatedTimestamps) {
        final TimestampGenerator tsgen = new StepTimestampGenerator(startTime, step, percount);
        for (final long ts : generatedTimestamps) {
            Assert.assertEquals(tsgen.next(), ts);
        }
    }
}
