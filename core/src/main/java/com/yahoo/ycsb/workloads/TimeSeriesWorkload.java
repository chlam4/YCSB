package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.FloatByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;
import com.yahoo.ycsb.tsdb.TimestampGenerator;
import com.yahoo.ycsb.tsdb.ApproxRealTimestampGenerator;

public class TimeSeriesWorkload extends Workload {
    private static final TimestampGenerator tsGen = new ApproxRealTimestampGenerator(TimeUnit.NANOSECONDS);

    @Override
    public void init(Properties p) throws WorkloadException {
    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                "field1", tsGen.nextTimestamp(), new FloatByteIterator(1.1f));
        final List<DataPointWithMetricID> datapoints = new ArrayList<DataPointWithMetricID>();
        datapoints.add(dp);
        if (db.insertDatapoints("mydb", "measurement1", TimeUnit.NANOSECONDS,
                datapoints) == 0)
            return true;
        return false;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        // TODO Auto-generated method stub
        return false;
    }

}
