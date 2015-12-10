package com.yahoo.ycsb.tsdb;

import com.yahoo.ycsb.ByteIterator;

/**
 * A data point representation which consists of the metric id, the timestamp
 * and the value represented by a byte iterator.
 * @author lamc1
 *
 */
public class DataPointWithMetricID extends DataPoint {
    private final String metricId;
    
    public DataPointWithMetricID(String metricId, long timestamp, ByteIterator value) {
        super(timestamp, value);
        this.metricId = metricId;
    }
    
    public String getMetricId() {
        return metricId;
    }

    @Override
    public String toString() {
        return String.format("Metric: %s, Value: %s, Timestamp: %d", metricId, this.getValue(), this.getTimestamp());
    }
}
