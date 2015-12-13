package com.yahoo.ycsb.db;


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class lookupDate {
	
	static ConcurrentMap<Integer, ArrayList<Integer>> metaTable = new ConcurrentHashMap<Integer, ArrayList<Integer>>();
	static Connection connection = null;



	public static void main(String[] argv) {
		
		Statement statement = null;
		PreparedStatement preparedStatement, preparedStatement2 = null;
		ResultSet resultSet, resultSet2 = null;
		ArrayList<String> tableNames = new ArrayList<String>();
		SimpleDateFormat formatter = new SimpleDateFormat("MM.dd  hh:mm:ss a");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		

		System.out.println("-------- MySQL JDBC Connection Testing ------------");

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}

		System.out.println("MySQL JDBC Driver Registered!");
		
		try {
			connection = DriverManager
			.getConnection("jdbc:mysql://lglan070:3306/mnrnaTest2","root", "");

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			System.out.println("You made it, take control your database now!");
			try {
				
			//	getSqlString("5",1448560799L, 1448582398L,"field5");
				migrateTables();
			//	getTable("5",1448582399L, 1448582399L);
				System.exit(0);
				preparedStatement = connection
				          .prepareStatement("SELECT tableName  from metaTable where 1448536798 < maxTimestamp and  1448536798 > minTimestamp");
			
			      resultSet = preparedStatement.executeQuery();
			      while (resultSet.next()) {
			          // It is possible to get the columns via name
			          // also possible to get the columns via the column number
			          // which starts at 1
			          // e.g. resultSet.getSTring(2);
			    	  String tableName = resultSet.getString("tableName");
			    	  
			         System.out.println("table " + tableName );
			    	  System.exit(0);
			      }
			      for (String tableName : tableNames){
			    	   preparedStatement = connection
						          .prepareStatement("SELECT max(timestamp), min(timestamp)  FROM "+ tableName);
				         resultSet = preparedStatement.executeQuery();  
				         while (resultSet.next()){
				         
				         System.out.print(tableName+", " + formatter.format(new Date((long)resultSet.getInt(1)*1000))+", " + formatter.format(new Date((long)resultSet.getInt(2)*1000)));
				         preparedStatement = connection
						          .prepareStatement("INSERT INTO metaTable (tableName, maxTimestamp, minTimestamp) VALUES("+tableName.split("_")[1]+", "+resultSet.getInt(1)+", "+resultSet.getInt(2)+")");
				         }
				         System.out.println();
				          preparedStatement.executeUpdate();
			      }
			      
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Failed to make connection!");
		}
	  }

	static private void  migrateTables(){
		
		
		try {
			 for (int i=1; i<3; i++){
				 System.out.print("mydb_"+i+", ");
				 for(int j=0; j<6; j++){
					 
				 String tableName = "mydb_"+i+"_"+j;
		        // System.out.println("table " + tableName );
				 
		      String qs = "INSERT into "+ tableName +" SELECT * FROM mnrnaTest."+tableName+" ORDER BY variable, timestamp";
		      System.out.println(qs);
				 PreparedStatement preparedStatement = connection
					          .prepareStatement(qs);
					          		
					        		  preparedStatement.executeUpdate();  
			        // while (resultSet.next()){
			        	 
			      //   System.out.print(resultSet.getInt(1)+", "+formatter.format(new Date((long)resultSet.getInt(2)*1000))+"-" + formatter.format(new Date((long)resultSet.getInt(3)*1000))+", ");
			       //  }
			         
			       
		      }
				 
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	static private String getSqlString(String group, long startTime,
		long endTime, String field) {
        ArrayList<Integer> groupList;
		String tableName = null;
		if (metaTable.isEmpty()) {
			try {
				PreparedStatement preparedStatement = connection
						.prepareStatement("SELECT * FROM metaTable");
				ResultSet resultSet = preparedStatement.executeQuery();
				while (resultSet.next()) {
					groupList = new ArrayList<Integer>();
					groupList.add(resultSet.getInt(2));
					groupList.add(resultSet.getInt(3));
					metaTable.put(resultSet.getInt(1), groupList);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		StringBuffer buff = new StringBuffer();
		for (Integer key : metaTable.keySet()) {
			if (startTime < metaTable.get(key).get(0)
					&& startTime >= metaTable.get(key).get(1)) {
				tableName = "mydb_" + key.intValue() + "_" + group;
				buff.append("SELECT variable FROM " + tableName
						+ " WHERE variable = '" + field + "' and timestamp >= "
						+ startTime);
				if (endTime > metaTable.get(key).get(0)) {
					buff.append(" AND timestamp <= "
							+ metaTable.get(key).get(1));
					buff.append(" UNION ");
					Integer key2 = key + 1;
					tableName = "mydb_" + key2 + "_" + group;
					long minStart = metaTable.get(key2).get(1);
					buff.append("SELECT variable FROM " + tableName
							+ " WHERE variable = '" + field
							+ "' and timestamp >= " + minStart);
					buff.append(" AND timestamp <= " + endTime);
				} else {
					buff.append(" AND timestamp <= " + endTime);
				}
			}
		}
		return buff.toString();
	}
}
