package com.yahoo.ycsb.tsdb;

import com.yahoo.ycsb.ByteIterator;

/**
 * A data point representation which consists of the metric id, the timestamp
 * and the value represented by a byte iterator.
 * @author lamc1
 *
 */
public class DataPointWithMetricID extends DataPoint {
    private final long metricId;
    private final String metricName;
    
    public DataPointWithMetricID(final long metricId, final String metricName, long timestamp, ByteIterator value) {
        super(timestamp, value);
        this.metricId = metricId;
        this.metricName = metricName;
    }
    
    public long getMetricId() {
        return metricId;
    }

    public String getMetricName() {
        return metricName;
    }

    @Override
    public String toString() {
        return String.format("Metric: %s, Value: %s, Timestamp: %d", metricId, this.getValue(), this.getTimestamp());
    }
}
