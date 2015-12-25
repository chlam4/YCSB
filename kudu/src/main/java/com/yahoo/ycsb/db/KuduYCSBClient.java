/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.stumbleupon.async.TimeoutException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;
import com.yahoo.ycsb.workloads.CoreWorkload;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import static org.kududb.Type.STRING;

/**
 * Kudu client for YCSB framework. Example to load: <blockquote>
 * 
 * <pre>
 * <code>
 * $ ./bin/ycsb load kudu -P workloads/workloada -threads 5 
 * </code>
 * </pre>
 * 
 * </blockquote> Example to run:  <blockquote>
 * 
 * <pre>
 * <code>
 * ./bin/ycsb run kudu -P workloads/workloada -p kudu_sync_ops=true -threads 5
 * </code>
 * </pre>
 * 
 * </blockquote>
 */
public class KuduYCSBClient extends com.yahoo.ycsb.DB {
  public static final String KEY = "key";
  public static final Status TIMEOUT =
      new Status("TIMEOUT", "The operation timed out.");
  public static final int MAX_TABLETS = 9000;
  public static final long DEFAULT_SLEEP = 60000;
  private static final String SYNC_OPS_OPT = "kudu_sync_ops";
  private static final String DEBUG_OPT = "kudu_debug";
  private static final String PRINT_ROW_ERRORS_OPT = "kudu_print_row_errors";
  private static final String PRE_SPLIT_NUM_TABLETS_OPT =
      "kudu_pre_split_num_tablets";
  private static final String TABLE_NUM_REPLICAS = "kudu_table_num_replicas";
  private static final String BLOCK_SIZE_OPT = "kudu_block_size";
  private static final String MASTER_ADDRESSES_OPT = "kudu_master_addresses";
  private static final int BLOCK_SIZE_DEFAULT = 4096;
  private static final List<String> COLUMN_NAMES = new ArrayList<String>();
  private static KuduClient client;
  private static Schema schema, tsSchema;
  private static int fieldCount;
  private boolean debug = false;
  private boolean printErrors = false;
  private String tableName;
  private KuduSession session;
  private KuduTable kuduTable;
  // Definition of a schema to test time-series workload
  private static enum TimeSeriesSchema {metric, event_time, value};
  // cleanup flags
  private static final String START_CLEAN_OPT = "kudu_start_clean";
  private boolean startClean = false;

  @Override
  public void init() throws DBException {
    if (getProperties().getProperty(DEBUG_OPT) != null) {
      this.debug = getProperties().getProperty(DEBUG_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors =
          getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors =
          getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    if (getProperties().getProperty(START_CLEAN_OPT) != null) {
      this.startClean = getProperties().getProperty(START_CLEAN_OPT).equals("true");
    }
    this.tableName = com.yahoo.ycsb.workloads.CoreWorkload.table;
    initClient(debug, tableName, getProperties(), startClean);
    this.session = client.newSession();
    if (getProperties().getProperty(SYNC_OPS_OPT) != null
        && getProperties().getProperty(SYNC_OPS_OPT).equals("false")) {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
      this.session.setMutationBufferSpace(100);
    } else {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    }

    if (tableName != null && !tableName.isEmpty()) {
      try {
        this.kuduTable = client.openTable(tableName);
      } catch (Exception e) {
        throw new DBException("Could not open a table because of:", e);
      }
    }
  }

  private static synchronized void initClient(boolean debug, String tableName,
      Properties prop, final boolean startClean) throws DBException {
    if (client != null) {
      return;
    }

    String masterAddresses = prop.getProperty(MASTER_ADDRESSES_OPT);
    if (masterAddresses == null) {
      masterAddresses = "localhost:7051";
    }

    int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
    if (numTablets > MAX_TABLETS) {
      throw new DBException("Specified number of tablets (" + numTablets
          + ") must be equal " + "or below " + MAX_TABLETS);
    }

    int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, 3);

    int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);

    client = new KuduClient.KuduClientBuilder(masterAddresses)
        .defaultSocketReadTimeoutMs(DEFAULT_SLEEP)
        .defaultOperationTimeoutMs(DEFAULT_SLEEP)
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP).build();
    if (debug) {
      System.out.println("Connecting to the masters at " + masterAddresses);
    }

    if (startClean) {
      System.out.println("Starting up clean and deleting all existing tables");
      try {
        for (final String table : client.getTablesList().getTablesList()) {
          if (debug) {
            System.out.println("Deleting table " + table);
          }
          client.deleteTable(table);
        }
      } catch (Exception e) {
        throw new DBException("Couldn't clean up the database", e);
      }
    }
    fieldCount = getIntFromProp(prop, CoreWorkload.FIELD_COUNT_PROPERTY,
        Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(fieldCount + 1);

    ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
        .key(true).desiredBlockSize(blockSize).build();
    columns.add(keyColumn);
    COLUMN_NAMES.add(KEY);
    for (int i = 0; i < fieldCount; i++) {
      String name = "field" + i;
      COLUMN_NAMES.add(name);
      columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING)
          .desiredBlockSize(blockSize).build());
    }
    schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions();
    builder.setNumReplicas(numReplicas);
    // create n-1 split keys, which will end up being n tablets master-side
    for (int i = 1; i < numTablets + 0; i++) {
      // We do +1000 since YCSB starts at user1.
      int startKeyInt = (MAX_TABLETS / numTablets * i) + 1000;
      String startKey = String.format("%04d", startKeyInt);
      PartialRow splitRow = schema.newPartialRow();
      splitRow.addString(0, "user" + startKey);
      builder.addSplitRow(splitRow);
    }

    if (tableName != null && !tableName.isEmpty()) {
      try {
        client.createTable(tableName, schema, builder);
      } catch (Exception e) {
        if (!e.getMessage().contains("ALREADY_PRESENT")) {
          throw new DBException("Couldn't create the table", e);
        }
      }
    }

    // create schema and tables for time-series workload
    final List<ColumnSchema> tsColumns = new ArrayList<ColumnSchema>(TimeSeriesSchema.values().length);
    final ColumnSchema metricId = new ColumnSchema.ColumnSchemaBuilder(
        TimeSeriesSchema.metric.name(), org.kududb.Type.INT64).key(true).desiredBlockSize(blockSize).build();
    final ColumnSchema timestamp = new ColumnSchema.ColumnSchemaBuilder(TimeSeriesSchema.event_time.name(),
        org.kududb.Type.TIMESTAMP).key(true).desiredBlockSize(blockSize).build();
    final ColumnSchema value = new ColumnSchema.ColumnSchemaBuilder(
        TimeSeriesSchema.value.name(), org.kududb.Type.FLOAT).desiredBlockSize(blockSize).build();
    tsColumns.add(metricId);
    tsColumns.add(timestamp);
    tsColumns.add(value);
    tsSchema = new Schema(tsColumns);

    final String tablePrefix = prop.getProperty("tsdb.table.prefix", "mydb");
    final int tableCount = Integer.parseInt(prop.getProperty("tsdb.table.count", "1"));
    for (int i=0; i<tableCount; i++) {
      try {
        client.createTable(tablePrefix+i, tsSchema, builder);
      } catch (Exception e) {
        if (!e.getMessage().contains("ALREADY_PRESENT")) {
          throw new DBException("Couldn't create the table", e);
        }
      }
    }
    
  }

  private static int getIntFromProp(Properties prop, String propName,
      int defaultValue) throws DBException {
    String intStr = prop.getProperty(propName);
    if (intStr == null) {
      return defaultValue;
    } else {
      try {
        return Integer.valueOf(intStr);
      } catch (NumberFormatException ex) {
        throw new DBException(
            "Provided number for " + propName + " isn't a valid integer");
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.session.close();
    } catch (Exception e) {
      throw new DBException("Couldn't cleanup the session", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results =
        new Vector<HashMap<String, ByteIterator>>();
    final Status status = scan(table, key, 1, fields, results);
    if (!status.equals(Status.OK)) {
      return status;
    }
    if (results.size() != 1) {
      return Status.NOT_FOUND;
    }
    result.putAll(results.firstElement());
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      KuduScanner.KuduScannerBuilder scannerBuilder =
          client.newScannerBuilder(this.kuduTable);
      List<String> querySchema;
      if (fields == null) {
        querySchema = COLUMN_NAMES;
        // No need to set the projected columns with the whole schema.
      } else {
        querySchema = new ArrayList<String>(fields);
        scannerBuilder.setProjectedColumnNames(querySchema);
      }

      PartialRow lowerBound = schema.newPartialRow();
      lowerBound.addString(0, startkey);
      scannerBuilder.lowerBound(lowerBound);
      if (recordcount == 1) {
        PartialRow upperBound = schema.newPartialRow();
        // Keys are fixed length, just adding something at the end is safe.
        upperBound.addString(0, startkey.concat(" "));
        scannerBuilder.exclusiveUpperBound(upperBound);
      }

      KuduScanner scanner = scannerBuilder.limit(recordcount) // currently noop
          .build();

      while (scanner.hasMoreRows()) {
        RowResultIterator data = scanner.nextRows();
        addAllRowsToResult(data, recordcount, querySchema, result);
        if (recordcount == result.size()) {
          break;
        }
      }
      RowResultIterator closer = scanner.close();
      addAllRowsToResult(closer, recordcount, querySchema, result);
    } catch (TimeoutException te) {
      if (printErrors) {
        System.err.println(
            "Waited too long for a scan operation with start key=" + startkey);
      }
      return TIMEOUT;
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void addAllRowsToResult(RowResultIterator it, int recordcount,
      List<String> querySchema, Vector<HashMap<String, ByteIterator>> result)
          throws Exception {
    RowResult row;
    HashMap<String, ByteIterator> rowResult =
        new HashMap<String, ByteIterator>(querySchema.size());
    if (it == null) {
      return;
    }
    while (it.hasNext()) {
      if (result.size() == recordcount) {
        return;
      }
      row = it.next();
      int colIdx = 0;
      for (String col : querySchema) {
        rowResult.put(col, new StringByteIterator(row.getString(colIdx)));
        colIdx++;
      }
      result.add(rowResult);
    }
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    Update update = this.kuduTable.newUpdate();
    PartialRow row = update.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      String columnName = schema.getColumnByIndex(i).getName();
      if (values.containsKey(columnName)) {
        String value = values.get(columnName).toString();
        row.addString(columnName, value);
      }
    }
    apply(update);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    Insert insert = this.kuduTable.newInsert();
    PartialRow row = insert.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      row.addString(i, new String(
          values.get(schema.getColumnByIndex(i).getName()).toArray()));
    }
    apply(insert);
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    Delete delete = this.kuduTable.newDelete();
    PartialRow row = delete.getRow();
    row.addString(KEY, key);
    apply(delete);
    return Status.OK;
  }

  private void apply(Operation op) {
    try {
      OperationResponse response = session.apply(op);
      if (response != null && response.hasRowError() && printErrors) {
        System.err.println("Got a row error " + response.getRowError());
      }
    } catch (Exception ex) {
      if (printErrors) {
        System.err.println("Failed to apply an operation " + ex.toString());
        ex.printStackTrace();
      }
    }
  }

  @Override
  public Status insertDatapoints(final String table, final String measurement,
      TimeUnit timeUnit, final List<DataPointWithMetricID> datapoints) {
    try {
      final KuduTable kt = client.openTable(table);
      for (final DataPointWithMetricID dp : datapoints) {
        final Insert insert = kt.newInsert();
        final PartialRow row = insert.getRow();
        row.addLong(TimeSeriesSchema.metric.name(), dp.getMetricId());
        row.addLong(TimeSeriesSchema.event_time.name(), dp.getTimestamp());
        row.addFloat(TimeSeriesSchema.value.name(), dp.getValue());
        apply(insert);
        if (debug) {
          System.out.println(String.format("Inserted metric %d timestamp %d value %f",
              dp.getMetricId(), dp.getTimestamp(), dp.getValue()));
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Could not open a table because of:" + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status scanDatapoints(final String table, final long metricId, final String measurement, final String field,
      final long startTime, final long endTime, final TimeUnit timeUnit, final Vector<DataPoint> result) {
    try {
      final KuduTable kt = client.openTable(table);
      KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kt);
//      scannerBuilder.setProjectedColumnNames(java.util.Arrays.asList(
//          TimeSeriesSchema.event_time.name(), TimeSeriesSchema.value.name()));

      //
      // Setting appropriate lower/upper bounds to get data for the given metric id only
      //
      final PartialRow lowerBound = tsSchema.newPartialRow();
      lowerBound.addLong(TimeSeriesSchema.metric.ordinal(), metricId);
      lowerBound.addLong(TimeSeriesSchema.event_time.ordinal(), startTime);
      scannerBuilder.lowerBound(lowerBound);
      final PartialRow upperBound = tsSchema.newPartialRow();
      upperBound.addLong(TimeSeriesSchema.metric.ordinal(), metricId);
      upperBound.addLong(TimeSeriesSchema.event_time.ordinal(), endTime);
      scannerBuilder.exclusiveUpperBound(upperBound);
      //
      // Setting column range filter for the desired time range
      //
      final ColumnSchema timeColumnSchema = tsSchema.getColumnByIndex(TimeSeriesSchema.event_time.ordinal());
      final ColumnRangePredicate rangePredicate = new ColumnRangePredicate(timeColumnSchema);
      rangePredicate.setLowerBound(startTime);
      rangePredicate.setUpperBound(endTime);
      scannerBuilder.addColumnRangePredicate(rangePredicate);

      KuduScanner scanner = scannerBuilder.build();
      while (scanner.hasMoreRows()) {
        RowResultIterator data = scanner.nextRows();
        addAllRowsToDataPointResult(data, result);
      }
      RowResultIterator closer = scanner.close();
      addAllRowsToDataPointResult(closer, result);
      if (debug) {
        System.out.println(String.format("Query result for metric %d between %d and %d: %s",
            metricId, startTime, endTime, result));
      }
    } catch (TimeoutException te) {
      if (printErrors) {
        System.err.println("Waited too long while scanning datapoints on metricId=" +
            metricId + " between " + startTime + " and " + endTime);
      }
      return TIMEOUT;
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void addAllRowsToDataPointResult(RowResultIterator it, Vector<DataPoint> result) throws Exception {
    if (it == null) {
      return;
    }
    for (final RowResult row : it) {
      final long ts = row.getLong(TimeSeriesSchema.event_time.ordinal());
      final float v = row.getFloat(TimeSeriesSchema.value.ordinal());
      result.add(new DataPoint(ts, v));
    }
  }
}