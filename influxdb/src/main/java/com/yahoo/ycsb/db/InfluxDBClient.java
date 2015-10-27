package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

public class InfluxDBClient extends DB {
    private InfluxDB influxDB;
    private int batchSize, batchInterval;

    @Override
    public void init() {
        final Properties props = getProperties();
        final String url = props.getProperty("influxdb.url", "http://localhost:8086");
        final String user = props.getProperty("influxdb.user", "user");
        final String password = props.getProperty("influxdb.password", "password");
        batchSize = Integer.parseInt(props.getProperty("influxdb.batchsize", "1"));
        batchInterval = Integer.parseInt(props.getProperty("influxdb.batchinterval", "1"));

        influxDB = InfluxDBFactory.connect(url, user, password);
        //
        // The batching feature seems broken in this java client that some
        // records will be lost if the batch size is 10000 or bigger.
        //
        if (batchSize > 1)  influxDB.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void cleanup() {
        //
        // Disabling batching will also flush the remaining points in the
        // outstanding batch into the database, thereby helping complete
        // all insertions in the test.
        //
        influxDB.disableBatch();
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int insertDatapoints(final String table, final String measurement, final List<DataPointWithMetricID> datapoints) {
        for (final DataPointWithMetricID dp : datapoints) {
            final Point p = Point.measurement(measurement)
                    .time(dp.getTimestamp(), TimeUnit.NANOSECONDS)
                    .field(dp.getMetricId(), dp.getValue()).build();
            influxDB.write(table, "default", p);
        }
        return 0;
    }
}
