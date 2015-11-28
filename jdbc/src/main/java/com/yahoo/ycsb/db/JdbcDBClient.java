/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 * 
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 * 
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore, 
 * only one index on the primary key is needed.
 * 
 * <p> The following options must be passed when using this database client.
 * 
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *  
 * @author sudipto, morand
 *
 */
public class JdbcDBClient extends DB implements JdbcDBClientConstants {
	
  private ArrayList<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private Integer jdbcFetchSize;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private static final Map<String, Boolean> tableCreatedMap = new ConcurrentHashMap<String, Boolean>();
  private static final Map<String, BufferedWriter> fileCreatedMap = new ConcurrentHashMap<String, BufferedWriter>();
  
  private static final Random rand = new Random();
  /**
   * The statement type for the prepared statements.
   */
  private static class StatementType {
    
    enum Type {
      INSERT(1),
      DELETE(2),
      READ(3),
      UPDATE(4),
      SCAN(5)
      ;
      int internalType;
      private Type(int type) {
        internalType = type;
      }
      
      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }
    
    Type type;
    int shardIndex;
    int numFields;
    String tableName;
    
    StatementType(Type type, String tableName, int numFields, int _shardIndex) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
      this.shardIndex = _shardIndex;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result
          + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      StatementType other = (StatementType) obj;
      if (numFields != other.numFields)
        return false;
      if (shardIndex != other.shardIndex)
        return false;
      if (tableName == null) {
        if (other.tableName != null)
          return false;
      } else if (!tableName.equals(other.tableName))
        return false;
      if (type != other.type)
        return false;
      return true;
    }
  }

    /**
     * For the given key, returns what shard contains data for this key
     *
     * @param key Data key to do operation on
     * @return Shard index
     */
    private int getShardIndexByKey(String key) {
       int ret = Math.abs(key.hashCode()) % conns.size();
       //System.out.println(conns.size() + ": Shard instance for "+ key + " (hash  " + key.hashCode()+ " ) " + " is " + ret);
       return ret;
    }

    /**
     * For the given key, returns Connection object that holds connection
     * to the shard that contains this key
     *
     * @param key Data key to get information for
     * @return Connection object
     */
    private Connection getShardConnectionByKey(String key) {
        return conns.get(getShardIndexByKey(key));
    }

    private void cleanupAllConnections() throws SQLException {
       for(Connection conn: conns) {
           conn.close();
       }
    }
  
  /**
   * Initialize the database connection and set it up for sending requests to the database.
   * This must be called once per client.
   * @throws  
   */
  @Override
	public void init() throws DBException {
		if (initialized) {
		  System.err.println("Client connection already initialized.");
		  return;
		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);

      String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
          if (jdbcFetchSizeStr != null) {
          try {
              this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
          } catch (NumberFormatException nfe) {
              System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
              throw new DBException(nfe);
          }
      }

      String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
      Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

      try {
		  if (driver != null) {
	      Class.forName(driver);
	    }
          int shardCount = 0;
          conns = new ArrayList<Connection>(3);
          for (String url: urls.split(",")) {
              System.out.println("Adding shard node URL: " + url);
            Connection conn = DriverManager.getConnection(url, user, passwd);

            // Since there is no explicit commit method in the DB interface, all
            // operations should auto commit, except when explicitly told not to
            // (this is necessary in cases such as for PostgreSQL when running a
            // scan workload with fetchSize)
            conn.setAutoCommit(autoCommit);

            shardCount++;
            conns.add(conn);
          }

          System.out.println("Using " + shardCount + " shards");

		  cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
		} catch (ClassNotFoundException e) {
		  System.err.println("Error in initializing the JDBS driver: " + e);
		  throw new DBException(e);
		} catch (SQLException e) {
		  System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
		initialized = true;
	}
	
  /*@Override
	public void cleanup() throws DBException {
	  try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
	}
	*/
	
	private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
	throws SQLException {
	  StringBuilder insert = new StringBuilder("INSERT INTO ");
	  insert.append(insertType.tableName);
	  insert.append(" VALUES(?");
    for (int i = 0; i < insertType.numFields; i++) {
      insert.append(",?");
    }
    insert.append(");");
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) return insertStatement;
    else return stmt;
	}
	
	
	private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
	throws SQLException {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.tableName);
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?;");
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) return readStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
	throws SQLException {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.tableName);
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?;");
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) return deleteStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
	throws SQLException {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.tableName);
    update.append(" SET ");
    for (int i = 1; i <= updateType.numFields; i++) {
      update.append(COLUMN_PREFIX);
      update.append(i);
      update.append("=?");
      if (i < updateType.numFields) update.append(", ");
    }
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?;");
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update.toString());
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) return insertStatement;
    else return stmt;
  }
	
	private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
	throws SQLException {
	  StringBuilder select = new StringBuilder("SELECT * FROM ");
    select.append(scanType.tableName);
    select.append(" WHERE ");
    select.append(PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(PRIMARY_KEY);
    select.append(" LIMIT ?;");
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select.toString());
    if (this.jdbcFetchSize != null) scanStatement.setFetchSize(this.jdbcFetchSize);
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) return scanStatement;
    else return stmt;
  }

	@Override
	public int read(String tableName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
	  if (tableName == null) {
      return -1;
    }
    if (key == null) {
      return -1;
    }
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return 1;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return SUCCESS;
    } catch (SQLException e) {
        System.err.println("Error in processing read of table " + tableName + ": "+e);
      return -2;
    }
	}

	@Override
	public int scan(String tableName, String startKey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
	  if (tableName == null) {
      return -1;
    }
    if (startKey == null) {
      return -1;
    }
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      return SUCCESS;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return -2;
    }
	}

	@Override
	public int update(String tableName, String key, HashMap<String, ByteIterator> values) {
	  if (tableName == null) {
      return -1;
    }
    if (key == null) {
      return -1;
    }
    try {
      int numFields = values.size();
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, numFields, getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        updateStatement.setString(index++, entry.getValue().toString());
      }
      updateStatement.setString(index, key);
      int result = updateStatement.executeUpdate();
      if (result == 1) return SUCCESS;
      else return 1;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return -1;
    }
	}

	@Override
	public int insert(String tableName, String key, HashMap<String, ByteIterator> values) {
	  if (tableName == null) {
	    return -1;
	  }
	  if (key == null) {
	    return -1;
	  }
	  try {
		  
	    int numFields = values.size();
	    StatementType type = new StatementType(StatementType.Type.INSERT, tableName, numFields, getShardIndexByKey(key));
	    PreparedStatement insertStatement = cachedStatements.get(type);
	    if (insertStatement == null) {
	      insertStatement = createAndCacheInsertStatement(type, key);
	    }
      insertStatement.setString(1, key);
      int index = 2;
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String field = entry.getValue().toString();
        insertStatement.setString(index++, field);
      }
      System.out.println("InsertStatement: "+ insertStatement.toString());
      int result = insertStatement.executeUpdate();
      if (result == 1) return SUCCESS;
      else return 1;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      System.exit(1);
      return -1;
    }
	}
	
	
	public int insert2Keys(String tableName, String key, long key1,
			HashMap<String, ByteIterator> values) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}

		int numFields = values.size() + 1;
		StatementType type = new StatementType(StatementType.Type.INSERT,
				tableName, numFields, getShardIndexByKey(key + key1));
		PreparedStatement insertStatement = cachedStatements.get(type);
		try {
			if (insertStatement == null) {
				insertStatement = createAndCacheInsertStatement(type, key);
			}

			insertStatement.setString(1, key);
			insertStatement.setLong(2, key1);
			int index = 3;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				insertStatement.setString(index++, field);
			}

			int result = insertStatement.executeUpdate();
			if (result == 1)
				return SUCCESS;
			else
				return 1;
		} catch (SQLException e) {
			System.err.println("Error in processing insert to table: "
					+ tableName + e);
			return -1;
		}
	}

	@Override
	public int delete(String tableName, String key) {
	  if (tableName == null) {
      return -1;
    }
    if (key == null) {
      return -1;
    }
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result == 1) return SUCCESS;
      else return 1;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return -1;
    }
	}
	  /**
     * Create a database table if it doesn't exist.
     * @param table Name of the database table
     */
    private void createTableIfNotExists(final String table) throws SQLException {
        final Boolean tableCreated = tableCreatedMap.putIfAbsent(table, true);
        if (tableCreated == null || tableCreated != true) {
            JdbcDBCreateTable.createTable(getProperties(), table);
        }
    }
    
    /**
     * Create a database table if it doesn't exist.
     * @param table Name of the database table
     */
    private synchronized void createFileIfNotExists(final String fileName) throws IOException {
    	
        if (!fileCreatedMap.containsKey(fileName)) {
        	File file = new File("/data/disk10/mysql/textfiles/"+fileName);
        	if (file.exists()) {
        		file.delete();
				
			}
        	
        	file.createNewFile();

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
        	fileCreatedMap.put(fileName, bw);
        }
    }
    @Override
    public int insertDatapoints(final String table, final String measurement,
            TimeUnit timeUnit, final List<DataPointWithMetricID> datapoints) {
        try {
			createFileIfNotExists(table);
		} catch (Exception e) {
			System.err.println("Error in writing table: " + table + e);
		      return -1;
		}
       
      for (final DataPointWithMetricID dp : datapoints) {
    	  HashMap<String,ByteIterator>  dpMap = new HashMap<String,ByteIterator>();
		  dpMap.put("value", dp.getValue());
          try {
			writeTable(table, dp.getMetricId(), dp.getTimestamp()/1000, dpMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          
          dpMap.clear();
       }
        return 0;
    }
    
    private void writeTable (String table, String variable, long timeStamp, HashMap<String,ByteIterator> dpMap) throws IOException {
    	BufferedWriter bw = fileCreatedMap.get(table);
    	StringBuffer buff = new StringBuffer();
    	buff.append(variable+','+timeStamp);
    	for (Map.Entry<String, ByteIterator> entry : dpMap.entrySet()) {
			String field = entry.getValue().toString();
			buff.append(','+field);
		}
    //	System.out.println(buff.toString());
    	buff.append('\n');
    	bw.write(buff.toString());
    	
    	
    }
    
    @Override
    public void cleanup() throws DBException{
    	
    	for (Map.Entry<String, BufferedWriter> entry : fileCreatedMap.entrySet()) {
			try {
				entry.getValue().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new DBException();
			}
		}
    	
    	
    }
    
	
    /*
	@Override
    public int insertDatapoints(final String table, final String measurement,
            TimeUnit timeUnit, final List<DataPointWithMetricID> datapoints) {
        try {
			createTableIfNotExists(table);
		} catch (SQLException e) {
			System.err.println("Error in createing table: " + table + e);
		      return -1;
		}
       
      for (final DataPointWithMetricID dp : datapoints) {
    	  HashMap<String,ByteIterator>  dpMap = new HashMap<String,ByteIterator>();
		  dpMap.put("value", dp.getValue());
          insert2Keys(table, dp.getMetricId(), dp.getTimestamp()/1000, dpMap);
          dpMap.clear();
       }
        return 0;
    }
    */
/*
  private HashMap<String, ByteIterator> buildValues(String key) {        
    HashMap<String,ByteIterator> values = new HashMap<String,ByteIterator>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        //fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextInt());
      }
      values.put(fieldkey,data);
    }
    return values;
  }
  */
  
    @Override
    public int scanDatapoints(final String table, final String key, final String field,
            final long startTime, final long endTime, final TimeUnit timeUnit,
            final Vector<DataPoint> result) {
    	try{
       // createTableIfNotExists(table);
        final long startTimeInNano = TimeUnit.SECONDS.convert(startTime, timeUnit);
        final long endTimeInNano = TimeUnit.SECONDS.convert(endTime, timeUnit);
        final String qs = String.format(
                "SELECT %s FROM %s WHERE timestanmp >= %d AND timestamp <= %d", "variable",
                table, startTimeInNano, endTimeInNano);
        StatementType type = new StatementType(StatementType.Type.READ, table, 1, getShardIndexByKey(key));
        PreparedStatement readStatement = cachedStatements.get(type);
        if (readStatement == null) {
          readStatement = createAndCacheReadStatement(type, key);
        }
        readStatement.setString(1, key);
        ResultSet resultSet = readStatement.executeQuery();
        if (rand.nextInt(1000) == 0) {
            System.out.println(String.format("  Query: %s\n  Result: %s", qs, resultSet.toString()));
        }
    	} catch (SQLException e) {
			System.err.println("Error in scanDatapoints table: " + table + e);
		      return -1;
		}
        return 0;
    }
    
}
