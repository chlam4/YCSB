package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.FloatByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

public class TimeSeriesWorkload extends Workload {
    private final static AtomicInteger i = new AtomicInteger();

    @Override
    public void init(Properties p) throws WorkloadException {
    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final long ts = i.incrementAndGet();
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                "testField", ts, new FloatByteIterator(1.1f));
        final List<DataPointWithMetricID> datapoints = new ArrayList<DataPointWithMetricID>();
        datapoints.add(dp);
        if (db.insertDatapoints("mydb", "testMeasurement", datapoints) == 0) return true;
        return false;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        // TODO Auto-generated method stub
        return false;
    }

}
