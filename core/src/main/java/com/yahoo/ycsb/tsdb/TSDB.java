package com.yahoo.ycsb.tsdb;

import java.util.Iterator;

import com.yahoo.ycsb.DB;

/**
 * A layer for accessing a time series database to be benchmarked.
 *
 */
public abstract class TSDB extends DB {
    /**
     * Perform loading of a set of data points to the time series database.
     * @param table The name of the table
     * @param datapoints Data points to be inserted
     */
    public abstract void load(String table, Iterator<DataPointWithMetricID> datapoints);
    /**
     * Perform a query on the given metric over a period of time.
     * @param table The name of the table
     * @param metricId The id of the metric to be retrieved
     * @param startTime The start of the time period in milliseconds since epoch
     * @param endTime The end of the time period in milliseconds since epoch
     * @return An iterator of data points retrieved
     */
    public abstract Iterator<DataPoint> query(String table, String metricId, long startTime, long endTime);
    // TODO: Define an interface for retrieving multiple metrics
    // TODO: Define an interface for temporal aggregation
}
