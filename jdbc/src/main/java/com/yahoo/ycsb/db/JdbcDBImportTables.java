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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Utility class to create the table to be used by the benchmark.
 * 
 * @author sudipto
 *
 */
public class JdbcDBImportTables implements JdbcDBClientConstants {

  private static void usageMessage() {
    System.out.println("Create Table Client. Options:");
    System.out.println("  -p   key=value properties defined.");
    System.out.println("  -P   location of the properties file to load.");
    System.out.println("  -n   name of the table.");
    System.out.println("  -f   number of fields (default 10).");
  }
  
  public static void importTables(Properties props)
  throws SQLException {
    String driver = props.getProperty(DRIVER_CLASS);
    String username = props.getProperty(CONNECTION_USER);
    String password = props.getProperty(CONNECTION_PASSWD, "");
    String url = props.getProperty(CONNECTION_URL);
    int fieldcount = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, 
        FIELD_COUNT_PROPERTY_DEFAULT));
    
    if (driver == null || username == null || url == null) {
      throw new SQLException("Missing connection information.");
    }
    
    Connection conn = null;
    
    try {
      Class.forName(driver);
      
      conn = DriverManager.getConnection(url, username, password);
      Statement stmt = conn.createStatement();
    
  		
  		long t0 = System.currentTimeMillis();

  				try {
  	//	for (int i = 0; i < 4; i++) {
  	//		for (int j = 0; j < 6; j++) {

  	//			String tableName = "mydb_" + i + "_" + j;
 				
                     String tableName = "bigint_test_0_0";
                   JdbcDBCreateTable.createTable(props, tableName);
                    //tmt.execute("ALTER TABLE mydb_test_0_0 DISABLE KEYS");
                 // for (char c = 'a'; c <= 'd'; c++) {
                   //for (char c1   = 'a'; c1 <= 'z'; c1++) {
                     String loadString = "LOAD DATA LOCAL INFILE '/data/disk05/textfiles/"+tableName+"' INTO TABLE "+tableName+" FIELDS TERMINATED BY ','";
                   PreparedStatement preparedStatement = conn
  							.prepareStatement(loadString);
  					
  					ResultSet resultSet = preparedStatement.executeQuery();
  				
  		long t1 = System.currentTimeMillis();
  		long time = (t1-t0)/(1000);
  		System.out.println(tableName+" finished importing data time (seconds): "+String.valueOf(time));
               // if (tableName == "adv") break;
                //}
                //}
                //stmt.execute("ALTER TABLE mydb_test_0_0 ENABLE KEYS");
  	//	long t2 = System.currentTimeMillis();
  	//	long time = (t2-t0)/(1000);
  	//	System.out.println(" finished importing data time (seconds): "+String.valueOf(time));
                
  					

  				} catch (SQLException e) {
  					// TODO Auto-generated catch block
  					e.printStackTrace();
  				}
  		//	}
  		//}
  		
    } catch (ClassNotFoundException e) {
        throw new SQLException("JDBC Driver class not found.");
      } finally {
        if (conn != null) {
          System.out.println("Closing database connection.");
          conn.close();
        }
      }

  	
  }
    
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args.length == 0) {
      usageMessage();
      System.exit(0);
    }
    
    String tablename = null;
    int fieldcount = -1;
    Properties props = new Properties();
    Properties fileprops = new Properties();

    // parse arguments
    int argindex = 0;
    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println(e.getMessage());
          System.exit(0);
        }

        // Issue #5 - remove call to stringPropertyNames to make compilable
        // under Java 1.5
        for (Enumeration<?> e = myfileprops.propertyNames(); e
            .hasMoreElements();) {
          String prop = (String) e.nextElement();

          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else if (args[argindex].compareTo("-f") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        try {
          fieldcount = Integer.parseInt(args[argindex++]);
        } catch (NumberFormatException e) {
          System.err.println("Invalid number for field count");
          usageMessage();
          System.exit(1);
        }
      } else {
        System.out.println("Unknown option " + args[argindex]);
        usageMessage();
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      System.exit(0);
    }

    // overwrite file properties with properties from the command line

    // Issue #5 - remove call to stringPropertyNames to make compilable under
    // Java 1.5
    for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;
    
    
    
    if (fieldcount > 0) {
      props.setProperty(FIELD_COUNT_PROPERTY, String.valueOf(fieldcount));
    }
    
    try {
      importTables(props);
    } catch (SQLException e) {
      System.err.println("Error in creating table. " + e);
      System.exit(1);
    }
  }
}