package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

public class InfluxDBClient extends DB {
    private InfluxDB influxDB;

    @Override
    public void init() {
        final Properties props = getProperties();
        final String url = props.getProperty("influxdb.url", "http://localhost:8086");
        final String user = props.getProperty("influxdb.user", "user");
        final String password = props.getProperty("influxdb.password", "password");
        final int batchSize = Integer.parseInt(props.getProperty("influxdb.batchsize", "1"));
        final int batchInterval = Integer.parseInt(props.getProperty("influxdb.batchinterval", "1"));

        influxDB = InfluxDBFactory.connect(url, user, password);
        if (batchSize > 1)  influxDB.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
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

    public int loadTSData(String table, List<DataPointWithMetricID> datapoints) {
        BatchPoints batchPoints = BatchPoints
                .database(table)
//                .tag("async", "true")
                .retentionPolicy("default")
                .consistency(ConsistencyLevel.ALL)
                .build();
        for (DataPointWithMetricID dp : datapoints) {
            Point p = Point.measurement(dp.getMetricId())
                    .time(dp.getTimestamp(), TimeUnit.MILLISECONDS)
                    .field("value", 1.0).build();
            batchPoints.point(p);
            influxDB.write(table, "default", p);
        }
        //influxDB.write(batchPoints);
        return 0;
    }
}
