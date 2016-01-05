package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.tsdb.DataPoint;
import com.yahoo.ycsb.tsdb.DataPointWithMetricID;



/**
 * In CQLSH, create keyspace and table.  Something like:
 * 
 *     CREATE KEYSPACE ycsb 
 *     WITH REPLICATION = {
 *          'class' : 'SimpleStrategy', 
 *          'replication_factor': 1
 *     };
 * 
 *     CREATE TABLE IF NOT EXISTS mydb(
 *      resource text,
 *      metric text,
 *      date text,
 *      event_time timestamp,
 *      value float,
 *      PRIMARY KEY((resource, metric, date), event_time)
 *     );
 * 
 *     ALTER TABLE mydb WITH compaction = {
 *         'class': 'DateTieredCompactionStrategy',
 *         'timestamp_resolution':'MICROSECONDS',
 *         'base_time_seconds':'3600',
 *         'max_sstable_age_days':'365'
 *     };
 * 
 * Or with a simplified schema where the metric id is an integer (see intMetricIds flag) :
 * 
 *     CREATE KEYSPACE ycsb 
 *     WITH REPLICATION = {
 *          'class' : 'SimpleStrategy', 
 *          'replication_factor': 1
 *     };
 *      
 *     CREATE TABLE IF NOT EXISTS mydb10(
 *      metric bigint,
 *      event_time int,
 *      value float,
 *      PRIMARY KEY((metric), event_time)
 *     );
 *
 *     ALTER TABLE mydb10 WITH compaction = {
 *         'class': 'DateTieredCompactionStrategy',
 *         'timestamp_resolution':'MICROSECONDS',
 *         'base_time_seconds':'3600',
 *         'max_sstable_age_days':'1'
 *     };
 * 
 * 
 * @author SantoD4
 *
 */
public class CassandraCqlTsClient extends DB{

    protected static Cluster cluster = null;
    protected static Session session = null;

    private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

    public static final int OK = 0;
    public static final int ERR = -1;
    public static final int NOT_FOUND = -3;

    public static final String YCSB_KEY = "y_id";
    public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
    public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

    public static final String HOSTS_PROPERTY = "hosts";
    public static final String PORT_PROPERTY = "port";


    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static boolean _debug = true;
    private static boolean intMetricIds = false;
    
    private List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();

    private static int fetchSize = 500;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        System.out.println("Starting init...");
        //Keep track of number of calls to init (for later cleanup)
        initCount.incrementAndGet();

        //Synchronized so that we only have a single
        //  cluster/session instance for all the threads.
        synchronized (initCount) {

            //Check if the cluster has already been initialized
            if (cluster != null) {
                return;
            }

            try {

                _debug = Boolean.parseBoolean(getProperties().getProperty("cassandra.debug", "false"));
                intMetricIds = Boolean.parseBoolean(getProperties().getProperty("cassandra.intMetricIds", "false"));

                String host = getProperties().getProperty(HOSTS_PROPERTY);
                if (host == null) {
                    throw new DBException(String.format("Required property \"%s\" missing for CassandraCQLClient", HOSTS_PROPERTY));
                }
                String hosts[] = host.split(",");
                String port = getProperties().getProperty("port", "9042");
                if (port == null) {
                    throw new DBException(String.format("Required property \"%s\" missing for CassandraCQLClient", PORT_PROPERTY));
                }

                String username = getProperties().getProperty(USERNAME_PROPERTY);
                String password = getProperties().getProperty(PASSWORD_PROPERTY);

                String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

                readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
                writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

                fetchSize = Integer.parseInt(getProperties().getProperty("cassandra.fetchsize", "500"));

                // public void connect(String node) {}
                if ((username != null) && !username.isEmpty()) {
                    cluster = Cluster.builder()
                        .withCredentials(username, password)
                        .withPort(Integer.valueOf(port))
                        .addContactPoints(hosts).build();
                }
                else {
                    cluster = Cluster.builder()
                        .withPort(Integer.valueOf(port))
                        .addContactPoints(hosts).build();
                }

                //Update number of connections based on threads
                int threadcount = Integer.parseInt(getProperties().getProperty("threadcount","1"));
                cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, threadcount);

                //Set connection timeout 3min (default is 5s)
                cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(3*60*1000);
                //Set read (execute) timeout 3min (default is 12s)
                cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(3*60*1000);

                Metadata metadata = cluster.getMetadata();
                System.err.printf("Connected to cluster: %s\n", metadata.getClusterName());

                for (Host discoveredHost : metadata.getAllHosts()) {
                    System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                        discoveredHost.getDatacenter(),
                        discoveredHost.getAddress(),
                        discoveredHost.getRack());
                }

                session = cluster.connect(keyspace);

            } catch (Exception e) {
                throw new DBException(e);
            }
        }//synchronized
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
      //make sure everything has completed before exiting
 	System.out.println("Make sure all async commands writes have finished...");
 	for(ResultSetFuture future: futures){
    	  future.getUninterruptibly();
    	}

      synchronized(initCount) {
        final int curInitCount = initCount.decrementAndGet();
        if (curInitCount <= 0) {
          session.close();
          cluster.close();
          cluster = null;
          session = null;
        }
        if (curInitCount < 0) {
          // This should never happen.
          throw new DBException(
              String.format("initCount is negative: %d", curInitCount));
        }
      }
    } 

    @Override
    public Status read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key,
            HashMap<String, ByteIterator> values) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
	 public Status insertDatapoints(final String table, final String measurement,
	            TimeUnit timeUnit, final List<DataPointWithMetricID> datapoints) {    	
    	    	
        for (final DataPointWithMetricID dp : datapoints) {
        	
            try {
            	long evTime;
                Insert insertStmt = QueryBuilder.insertInto(table);
            	
        	    if (intMetricIds){
        	    	// just use one second granularity for event time
                    evTime = dp.getTimestamp()/1000;         	    	

        	    	// Strip off the "measurement" prefix
        	    	insertStmt.value("metric", Long.parseLong(measurement.substring(11)));
        	    	
        	    }else{
                    evTime = dp.getTimestamp();        	    	

        	    	//Covert to number of days since epoch
	        	    String date = Long.toString(dp.getTimestamp()/86400/1000000000); 
	        	    
	                //Add data
	                insertStmt.value("resource", measurement);
	                insertStmt.value("metric", dp.getMetricId());
	                insertStmt.value("date", date);
	                
        	    }
        	    insertStmt.value("event_time", evTime);
                
                ByteIterator byteIterator = dp.getValue();
                float value = Float.parseFloat(byteIterator.toString());
                insertStmt.value("value", value);
                
                if (_debug) {
                    System.out.println(insertStmt.toString());
                }

                ResultSetFuture resultSetFuture = session.executeAsync(insertStmt);
               // futures.add(resultSetFuture);

            } catch (Exception e) {
                e.printStackTrace();
                return Status.ERROR;
            }
        }
        return Status.OK;
    }

    @Override
    public Status scanDatapoints(final String table, final String key, final String field,
            final long startTime, final long endTime, final TimeUnit timeUnit,
            final Vector<DataPoint> result) {    	
        try {
            Statement stmt;
            Select.Builder selectBuilder;        	
            selectBuilder = QueryBuilder.select();
           
            // Assume the range is within a single day
            if (intMetricIds){
            	
                stmt = selectBuilder.from(table).where(QueryBuilder.eq("metric", Long.parseLong(key.substring(11))))
                		.and(QueryBuilder.gt("event_time", startTime/1000))
                		.and(QueryBuilder.lt("event_time", endTime/1000));            	
            }else {
            	String date = Long.toString(startTime/86400/1000); 
            	
                stmt = selectBuilder.from(table).where(QueryBuilder.eq("resource", key))
                		.and(QueryBuilder.eq("metric", field))
                		.and(QueryBuilder.eq("date", date))
                		.and(QueryBuilder.gt("event_time", startTime))
                		.and(QueryBuilder.lt("event_time", endTime));
            }
            

            stmt.setConsistencyLevel(readConsistencyLevel);

            if (_debug) {
              System.out.println(stmt.toString());
            }
            stmt.setFetchSize(fetchSize);
            ResultSetFuture future  = session.executeAsync(stmt);
            
            while (!future.isDone()) {}
            
            ResultSet rs = future.get();  // TODO: Shall we parse the query results?
            int rowCount =0;
	        Iterator<Row> iter = rs.iterator();
            while (iter.hasNext()){
                Row row = iter.next();
                rowCount++;
            }
            while (!rs.isFullyFetched()) {
                if (_debug) {
                    System.out.println("Fetching more...");
                }
            	rs.fetchMoreResults();
                Row row = iter.next();
                rowCount++;
              //  System.out.println(row);
            }
            if (_debug) {
                System.out.println("Row Count = "+ rowCount);
            }

            return Status.OK;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error reading key: " + key);
            return Status.ERROR;
        }
    }	
}
