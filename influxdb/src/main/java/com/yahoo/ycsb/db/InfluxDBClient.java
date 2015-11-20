package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

public class InfluxDBClient extends DB {
    private InfluxDB influxDB;
    private int batchSize, batchInterval;
    private final Map<String, Boolean> tableCreatedMap = new HashMap<String, Boolean>();
    private final Random rand = new Random();

    @Override
    public void init() {
        final Properties props = getProperties();
        final String url = props.getProperty("influxdb.url", "http://localhost:8086");
        final String user = props.getProperty("influxdb.user", "user");
        final String password = props.getProperty("influxdb.password", "password");
        batchSize = Integer.parseInt(props.getProperty("influxdb.batchsize", "1"));
        batchInterval = Integer.parseInt(props.getProperty("influxdb.batchinterval", "1"));

        influxDB = InfluxDBFactory.connect(url, user, password);
        if (batchSize > 1)  influxDB.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void cleanup() {
        //
        // Disabling batching will also flush the remaining points in the
        // outstanding batch into the database, thereby helping complete
        // all insertions in the test.
        //
        if (batchSize > 1)  influxDB.disableBatch();
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

    /**
     * Create a database table if it doesn't exist.
     * @param table Name of the database table
     */
    private synchronized void createTableIfNotExists(final String table) {
        final Boolean tableCreated = tableCreatedMap.putIfAbsent(table, true);
        if (tableCreated == null || tableCreated != true) {
            influxDB.createDatabase(table);
        }
    }

    @Override
    public int insertDatapoints(final String table, final String measurement,
            TimeUnit timeUnit, final List<DataPointWithMetricID> datapoints) {
        createTableIfNotExists(table);
        for (final DataPointWithMetricID dp : datapoints) {
            final Point p = Point.measurement(measurement)
                    .time(dp.getTimestamp(), timeUnit)
                    .field(dp.getMetricId(), dp.getValue()).build();
            influxDB.write(table, "default", p);
        }
        return 0;
    }

    @Override
    public int scanDatapoints(final String table, final String key, final String field,
            final long startTime, final long endTime, final TimeUnit timeUnit,
            final Vector<DataPoint> result) {
        createTableIfNotExists(table);
        final long startTimeInNano = TimeUnit.NANOSECONDS.convert(startTime, timeUnit);
        final long endTimeInNano = TimeUnit.NANOSECONDS.convert(endTime, timeUnit);
        final String qs = String.format(
                "SELECT %s FROM %s WHERE time >= %d AND time <= %d", field,
                key, startTimeInNano, endTimeInNano);
        final QueryResult queryResult = influxDB.query(new Query(qs, table)); // TODO: Shall we parse the query results?
        if (rand.nextInt(1000) == 0) {
            System.out.println(String.format("  Query: %s\n  Result: %s", qs, queryResult.toString()));
        }
        return 0;
    }
}
