package com.yahoo.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.FloatByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.FixedFloatGenerator;
import com.yahoo.ycsb.generator.FloatGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.RandomFloatGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;
import com.yahoo.ycsb.tsdb.RandomTimestampGenerator;
import com.yahoo.ycsb.tsdb.StepTimestampGenerator;
import com.yahoo.ycsb.tsdb.TimestampGenerator;

public class TimeSeriesWorkload extends Workload {
    private TimestampGenerator loadTimestampGenerator;
    private TimestampGenerator queryTimestampGenerator;
    private IntegerGenerator queryKeyGenerator;
    private FloatGenerator floatGenerator;
    private TimeUnit timeUnit;
    private long queryLength;   // in the specified timeUnit
    private String tablePrefix;
    private int tableCount, batchCount;
    private String measurementPrefix;
    private int measurementCount;
    private String fieldPrefix;
    private int fieldCount;
    private long recordCount;
    private final AtomicLong index = new AtomicLong();
    private static final Random rand = new Random();

    @Override
    public void init(Properties p) throws WorkloadException {
        //
        // tables, series (measurements), fields
        //
        tablePrefix = p.getProperty("tsdb.table.prefix", "mydb");
        tableCount = Integer.parseInt(p.getProperty("tsdb.table.count", "1"));
        batchCount = Integer.parseInt(p.getProperty("tsdb.table.batches", "0"));
        measurementPrefix = p.getProperty("tsdb.measurement.prefix", "measurement");
        measurementCount = Integer.parseInt(p.getProperty("tsdb.measurement.count", "1"));
        fieldPrefix = p.getProperty("tsdb.field.prefix", "field");
        fieldCount = Integer.parseInt(p.getProperty("tsdb.field.count", "1"));
        recordCount = Long.parseLong(p.getProperty("recordcount", "10000000"));
        //
        // floating point value generator
        //
        final String floatGeneratorName = p.getProperty("tsdb.floatGenerator", "Uniform");
        if (floatGeneratorName.equals("Fixed")) {
            final float fixed = Float.parseFloat(p.getProperty("tsdb.floatGenerator.fixed", "1.0"));
            System.out.println(String.format("Generating metric values all as a single fixed value %f.", fixed));
            floatGenerator = new FixedFloatGenerator(fixed);
        } else {    // default to "Uniform"
            final float lower = Float.parseFloat(p.getProperty("tsdb.floatGenerator.lower", "0.0"));
            final float upper = Float.parseFloat(p.getProperty("tsdb.floatGenerator.upper", "1.0"));
            System.out.println(String.format("Generating metric values uniformly random between %f and %f.", lower, upper));
            floatGenerator = new RandomFloatGenerator(lower, upper);
        }
        //
        // query parameters and time stamp generator
        //
        timeUnit = TimeUnit.valueOf(p.getProperty("tsdb.timeUnit", "MILLISECONDS"));
        queryLength = Long.parseLong(p.getProperty("tsdb.query.length", Long.toString(timeUnit.convert(1, TimeUnit.HOURS))));
        final Long currTime = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);  // in specified unit
        final long lowerbound = Long.parseLong(p.getProperty("tsdb.query.lowerbound", currTime.toString()));
        final long upperbound = Long.parseLong(p.getProperty("tsdb.query.upperbound", currTime.toString()));
        queryTimestampGenerator = new RandomTimestampGenerator(lowerbound, upperbound);
        final String queryKeyGeneratorName = p.getProperty("tsdb.query.keyGenerator", "Uniform");
        if (queryKeyGeneratorName.equals("Zipfian")) {
            final double zipfianConst = Double.parseDouble(p.getProperty("tsdb.query.keyGenerator.zipfianConstant", "0.99"));
            System.out.println(String.format("Generating query keys according to Zipfian distribution with Zipfian constant %f.", zipfianConst));
            queryKeyGenerator = new ZipfianGenerator(0, measurementCount * fieldCount - 1, zipfianConst);
        } else if (queryKeyGeneratorName.equals("ScrambledZipfian")) {
            final double zipfianConst = Double.parseDouble(p.getProperty("tsdb.query.keyGenerator.zipfianConstant", "0.99"));
            System.out.println(String.format("Generating query keys according to ScrambledZipfian distribution with Zipfian constant %f.", zipfianConst));
            queryKeyGenerator = new ScrambledZipfianGenerator(0, measurementCount * fieldCount - 1, zipfianConst);
        } else { // default to "Uniform"
            queryKeyGenerator = new UniformIntegerGenerator(0, measurementCount * fieldCount - 1);
        }
        //
        // loading time stamp generator
        //
//        final Long defaultStartTime = timeUnit.convert(TimeUnit.DAYS.convert(currTime, TimeUnit.MILLISECONDS), TimeUnit.DAYS);
//        final long startTime = Long.parseLong(p.getProperty("tsdb.timestamp.start", defaultStartTime.toString()));
//        final long pollingInterval = Integer.parseInt(p.getProperty("tsdb.timestamp.polling.interval", "240000"));  // 4 minutes
//        final int step = Integer.parseInt(p.getProperty("tsdb.timestamp.step", "10"));
//        final Long perStepCount = ((long) fieldCount * measurementCount * step - 1) / pollingInterval + 1;
//        System.out.println("PerstepCount:" +perStepCount+" step: "+step );
//        loadTimestampGenerator = new StepTimestampGenerator(startTime, step, perStepCount);
        final String loadTimestampGeneratorName = p.getProperty("tsdb.timestamp.generator", "Realtime");
        if (loadTimestampGeneratorName.equals("Step")) {
            final Long defaultStartTime = currTime;
            final long startTime = Long.parseLong(p.getProperty("tsdb.timestamp.start", defaultStartTime.toString()));
            final long pollingInterval = Integer.parseInt(p.getProperty("tsdb.timestamp.polling.interval", "240000")); // 4 minutes
            final int step = Integer.parseInt(p.getProperty("tsdb.timestamp.step", "10"));
            final Long perStepCount = ((long) fieldCount * measurementCount * step - 1) / pollingInterval + 1;
            System.out.println(String.format("Generating timestamps based on start time %d, polling interval %d and step size %d.", startTime, pollingInterval, step));
            System.out.println(String.format("PerstepCount %d.", perStepCount));
            loadTimestampGenerator = new StepTimestampGenerator(startTime, step, perStepCount);
        } else { // default to "Realtime"
            final Integer defaultPerStepCount = java.lang.Math.min(1000, measurementCount * fieldCount);
            final int perStepCount = Integer.parseInt(p.getProperty("tsdb.timestamp.perStepCount", defaultPerStepCount.toString()));
            System.out.println(String.format("Generating timestamps based real time with group size of %d.", perStepCount));
            loadTimestampGenerator = new StepTimestampGenerator(perStepCount, timeUnit);
        }
    }

    /**
     * Compute the corresponding table name given a long integer id.
     * @param id
     * @return Table name
     */
    private String getTableName(final long id) {
    	
    	if (batchCount == 0 ){
          return tablePrefix + (getMeasurementId(id)*fieldCount + getFieldId(id)) % tableCount;
    	}
    	else {
          long batchSize = recordCount/tableCount;
         
          //System.out.println(id+", "+recordCount+", "+batchSize+", "+id/batchSize);
    	  return tablePrefix + "_"+id/batchSize+"_"+(getMeasurementId(id)*fieldCount + getFieldId(id)) % batchCount;
    	}
    }

    /**
     * Compute the corresponding measurement name given a long integer id.
     * @param id
     * @return Measurement name
     */
    private String getMeasurementName(final long id) {
        return measurementPrefix + getMeasurementId(id);
    }
    private long getMeasurementId(final long id) {
        return (id/fieldCount)%measurementCount;
    }

    /**
     * Compute the corresponding field name given a long integer id.
     * @param id
     * @return Field name
     */
    private String getFieldName(final long id) {
        return fieldPrefix + getFieldId(id);
    }
    private long getFieldId(final long id) {
        return id%fieldCount;
    }

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        final long id = index.getAndIncrement();
        
        final String measurement = getMeasurementName(id);
        final String field = getFieldName(id);
        final String table = getTableName(id);
        final DataPointWithMetricID dp = new DataPointWithMetricID(
                field, loadTimestampGenerator.next(), new FloatByteIterator(floatGenerator.nextFloat()));
        final List<DataPointWithMetricID> datapoints = new ArrayList<DataPointWithMetricID>();
        datapoints.add(dp);
        if (db.insertDatapoints(table, measurement, timeUnit,
                datapoints) == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        long endTime = queryTimestampGenerator.next();
        //final int id = queryKeyGenerator.nextInt();
        final long id = (long)(rand.nextDouble()*fieldCount);
        //System.out.println(fieldCount+", "+id);
        final String table = getTableName(id);
        final String measurement = getMeasurementName(id);
        final String field = getFieldName(id);
        if (db.scanDatapoints(table, measurement, field, endTime-queryLength, endTime,
                timeUnit, new Vector<DataPoint>()) == 0) {
            return true;
        }
        return false;
    }
}
