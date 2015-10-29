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
import com.yahoo.ycsb.generator.FixedFloatGenerator;
import com.yahoo.ycsb.generator.FloatGenerator;
import com.yahoo.ycsb.generator.RandomFloatGenerator;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;
import com.yahoo.ycsb.tsdb.RandomTimestampGenerator;
import com.yahoo.ycsb.tsdb.TimestampGenerator;
import com.yahoo.ycsb.tsdb.ApproxRealTimestampGenerator;

public class TimeSeriesWorkload extends Workload {
    private static final TimestampGenerator loadTS = new ApproxRealTimestampGenerator(TimeUnit.NANOSECONDS);
    private FloatGenerator floatGenerator;
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
        final String floatGeneratorName = p.getProperty("tsdb.floatGenerator", "fixed");
        final float lower = Float.parseFloat(p.getProperty("tsdb.floatGenerator.lower", "0.0"));
        final float upper = Float.parseFloat(p.getProperty("tsdb.floatGenerator.upper", "1.0"));
        final float fixed = Float.parseFloat(p.getProperty("tsdb.floatGenerator.fixed", "1.0"));
        if (floatGeneratorName.equals("random")) {
            floatGenerator = new RandomFloatGenerator(lower, upper);
        } else {
            floatGenerator = new FixedFloatGenerator(fixed);
        }
        final long floorTS = Long.parseLong(p.getProperty("query.timestamp.lower", "0"));
        final long ceilingTS = Long.parseLong(p.getProperty("query.timestamp.upper", Long.toString(Long.MAX_VALUE)));
        queryTS = new RandomTimestampGenerator(floorTS, ceilingTS);
    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                field, loadTS.next(), new FloatByteIterator(floatGenerator.nextFloat()));
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
