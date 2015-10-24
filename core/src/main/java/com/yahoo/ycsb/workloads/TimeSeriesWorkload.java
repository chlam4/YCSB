package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

public class TimeSeriesWorkload extends Workload {
    final static AtomicInteger i = new AtomicInteger();

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final long ts = i.incrementAndGet();
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                "testMetric", ts, new StringByteIterator("testValue"));
        final List<DataPointWithMetricID> datapoints = new ArrayList<DataPointWithMetricID>();
        datapoints.add(dp);
        if (db.loadTSData("mydb", datapoints) == 0) return true;
        return false;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        // TODO Auto-generated method stub
        return false;
    }

}
