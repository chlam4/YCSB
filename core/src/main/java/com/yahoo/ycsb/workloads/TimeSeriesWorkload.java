package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.FloatByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;
import com.yahoo.ycsb.tsdb.RandomTimestampGenerator;
import com.yahoo.ycsb.tsdb.TimestampGenerator;
import com.yahoo.ycsb.tsdb.ApproxRealTimestampGenerator;

public class TimeSeriesWorkload extends Workload {
    private static final TimestampGenerator loadTS = new ApproxRealTimestampGenerator(TimeUnit.NANOSECONDS);
    private TimestampGenerator queryTS;
    private TimeUnit timeUnit;
    private String table;
    private String measurement;
    private String field;

    @Override
    public void init(Properties p) throws WorkloadException {
        timeUnit = TimeUnit.valueOf(p.getProperty("tsdb.timeUnit", "MILLISECONDS"));
        table = p.getProperty("tsdb.table", "mydb");
        measurement = p.getProperty("tsdb.measurement", "measurement1");
        field = p.getProperty("tsdb.field", "field1");
        final long floorTS = Long.parseLong(p.getProperty("query.timestamp.lowerBound", "0"));
        final long ceilingTS = Long.parseLong(p.getProperty("query.timestamp.upperBound", Long.toString(Long.MAX_VALUE)));
        queryTS = new RandomTimestampGenerator(floorTS, ceilingTS);
    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                field, loadTS.next(), new FloatByteIterator(1.1f));
        final List<DataPointWithMetricID> datapoints = new ArrayList<DataPointWithMetricID>();
        datapoints.add(dp);
        if (db.insertDatapoints(table, measurement, TimeUnit.NANOSECONDS,
                datapoints) == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        long startTime = queryTS.next();
        long endTime = queryTS.next();
        if (startTime > endTime) {
            final long tmp = startTime;
            startTime = endTime;
            endTime = tmp;
        }
        if (db.scanDatapoints(table, measurement, field, startTime, endTime,
                timeUnit, new Vector<DataPoint>()) == 0) {
            return true;
        }
        return false;
    }
}
