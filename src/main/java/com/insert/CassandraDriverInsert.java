package com.insert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraDriverInsert implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(CassandraDriverInsert.class.getName());
    public static ConcurrentHashMap<String, PreparedStatement> preparedStatementMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> insertQueryStatement = new ConcurrentHashMap<>();
    public static long timeMarker = 0;
    public static long processedRecords = 0;
    public static long failedRecords = 0;


    public static long getProcessedRecordCount() {
        return processedRecords;
    }

    public static long getFailedRecordCount() {
        return failedRecords;
    }

    /**
     * @param keySpace
     * @param tableName
     * @param columnNames
     * @param columnValues
     */
    public static void insert(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues, long recordCount) {
        Session session = null;
        try {
//            if (session == null) {
//                CassandraConnector.connect(9042);
//                session = CassandraConnector.getSession();
//                //System.out.println(" Created session " + session.getState());
//            }
            session = CassandraConnector.connect();
            System.out.println("Got Session");
            //CassandraDriverInsert();
            //System.out.println("Column names "+ columnNames.toString());
            //System.out.println("Column Values "+columnValues.toString());
            //System.out.println("Generating prepared statement");
            PreparedStatement prepared = getPreparedStatement(session, keySpace, tableName, columnNames);
            //System.out.println("Prepared Statement Generated"+ prepared.getQueryString());
            BoundStatement bound = prepared.bind();
            session.executeAsync(loadIngestionBoundStatement(columnNames, columnValues, bound));
            ++processedRecords;
            if (processedRecords % 1000000 == 0) {
                long previousTime = timeMarker;
                timeMarker = new Timestamp(System.currentTimeMillis()).getTime();
                LOGGER.info("Inserted 1000000 records in " + ((timeMarker - previousTime) / 1000) + " seconds" +
                        "Total Records inserted = " + processedRecords +
                        ", Total Failed Records = " + failedRecords);
            }
        } catch (Exception e) {
            ++failedRecords;
            LOGGER.error("[" + CassandraDriverInsert.class.getName() + "] Column List : " + columnNames.toString());
            LOGGER.error("[" + CassandraDriverInsert.class.getName() + "] Column Values : " + columnValues.toString());
            LOGGER.error("[" + CassandraDriverInsert.class.getName() + "] Exception occurred while trying to execute cassandra insert: " +
                    e.getMessage(), e);
        } finally {
//            if (processedRecords == recordCount) {
//                LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Processed all records : " + processedRecords);
//                CassandraConnector.closeSession(session);
//            }
            CassandraConnector.closeSession(session);
        }
    }

    /**
     * @param columnNames
     * @param columnValues
     * @param boundStatement
     * @return
     */
    public static BoundStatement loadBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            Object value = columnValues.get(i);
            if (value != null && value != "") {  // Skipping tombstones
                if (name.equalsIgnoreCase("ts")) {
                    boundStatement.setUUID(name, (UUID) value);
                } else {
                    boundStatement.setString(name, value.toString());
                }
            }
        }
        return boundStatement;
    }

    /**
     * @param columnNames
     * @param columnValues
     * @param boundStatement
     * @return
     */
    public static BoundStatement loadIngestionBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {

        ArrayList<String> names = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            try {
                String name = columnNames.get(i);
                Object value = columnValues.get(i);
                if (value != null && value != "") {  // Skipping tombstones
                    if (name.equals("raw_ts") || name.equals("ts")) {
                        boundStatement.setUUID(name, UUID.fromString(value.toString()));
                    } else if (value instanceof Integer) {
                        boundStatement.setInt(name, (Integer) value);
                    } else if (value instanceof Date) {
                        boundStatement.setTimestamp(name, (Date) value);
                    } else if (value instanceof UUID) {
                        boundStatement.setUUID(name, (UUID) value);
                    } else if (value instanceof Double) {
                        boundStatement.setDouble(name, (Double) value);
                    } else if (value instanceof Long || value instanceof BigInteger) {
                        boundStatement.setLong(name, (Long) value);
                    } else {
                        boundStatement.setString(name, value.toString());
                    }
                }
            } catch (Exception exception) {
                LOGGER.error("[" + CassandraDriverInsert.class.getName() + "] Exception occured in processing column : " + columnNames.get(i));
                LOGGER.error("[" + CassandraDriverInsert.class.getName() + "] Value of the column is : " + columnValues.get(i));
                throw exception;
            }
        }
        //System.out.println("Bound Statement "+ boundStatement.toString());
        return boundStatement;
    }

    /**
     * @param session
     * @param keyspace
     * @param tableName
     * @param columnNames
     * @return
     */
    public static PreparedStatement getPreparedStatement(final Session session, final String keyspace, final String tableName, final List<String> columnNames) {
        //System.out.println("Inside prepared statement");
        if (preparedStatementMap.get(tableName) == null) {
            preparedStatementMap.put(tableName, session.prepare(
                    prepareQueryString(keyspace, tableName, columnNames)));
        }
        //System.out.println("Outside prepared statement");
        return preparedStatementMap.get(tableName);
    }

    /**
     * @param keyspace
     * @param tableName
     * @param columnNames
     * @return
     */
    public static String prepareQueryString(final String keyspace, final String tableName, final List<String> columnNames) {
        if (insertQueryStatement.get(tableName) == null) {
            //System.out.println("Inside prepare Query String ");
            StringBuilder queryStringBuilder = new StringBuilder("INSERT INTO " + keyspace + "." + tableName + " (");
            StringBuilder valueBuilder = new StringBuilder("(");
            for (int i = 0; i < columnNames.size(); i++) {
                queryStringBuilder.append(columnNames.get(i));
                valueBuilder.append("?");
                if (i < columnNames.size() - 1) {
                    queryStringBuilder.append(", ");
                    valueBuilder.append(", ");
                } else {
                    queryStringBuilder.append(")");
                    valueBuilder.append(")");
                }
            }
            queryStringBuilder.append(" VALUES ").append(valueBuilder);
            //System.out.println("InsertQueryStatement " + queryStringBuilder.toString());
            //LOGGER.info("InsertQueryStatement " + queryStringBuilder.toString());
            insertQueryStatement.put(tableName, queryStringBuilder.toString());
        }
        //System.out.println("insert statement inside map is  " + insertQueryStatement.get(tableName));
        return insertQueryStatement.get(tableName);
    }


}
