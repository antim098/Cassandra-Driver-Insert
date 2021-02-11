package com.insert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraDriverInsert implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(CassandraDriverInsert.class.getName());
    public static ConcurrentHashMap<String, PreparedStatement> preparedStatementMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> insertQueryStatement = new ConcurrentHashMap<>();
    public static Session session = null;

    public CassandraDriverInsert() {
        if (session == null) {
            CassandraConnector.connect(9042);
            session = CassandraConnector.getSession();
        }
    }

    /**
     * @param keySpace
     * @param tableName
     * @param columnNames
     * @param columnValues
     */
    public void insert(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues, Boolean isIngestion) {
        try {
            //System.out.println("Column names "+ columnNames.toString());
            //System.out.println("Column Values "+columnValues.toString());
            PreparedStatement prepared = getPreparedStatement(session, keySpace, tableName, columnNames);
            System.out.println("Prepared Statement "+ prepared.getQueryString());
            BoundStatement bound = prepared.bind();
            if (isIngestion) {
                session.executeAsync(loadIngestionBoundStatement(columnNames, columnValues, bound));
            } else {
                session.executeAsync(loadBoundStatement(columnNames, columnValues, bound));
            }
        } catch (Exception e) {
            LOGGER.error("[" + this + "] Exception occurred while trying to execute cassandra insert: " +
                    e.getMessage(), e);
        } finally {
            //CassandraConnector.closeSession(session);
        }
    }

    /**
     * @param columnNames
     * @param columnValues
     * @param boundStatement
     * @return
     */
    private BoundStatement loadBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {
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
    private BoundStatement loadIngestionBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            Object value = columnValues.get(i);
            if (value != null && value != "") {  // Skipping tombstones
                if (value instanceof Date) {
                    boundStatement.setTimestamp(name, (Date) value);
                } else if (value instanceof Integer) {
                    boundStatement.setInt(name, (Integer) value);
                } else if (value instanceof UUID) {
                    boundStatement.setUUID(name, (UUID) value);
                } else {
                    boundStatement.setString(name, value.toString());
                }
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
    private PreparedStatement getPreparedStatement(final Session session, final String keyspace, final String tableName, final List<String> columnNames) {
        if (preparedStatementMap.get(tableName) == null) {
            preparedStatementMap.put(tableName, session.prepare(
                    prepareQueryString(keyspace, tableName, columnNames)));
        }
        return preparedStatementMap.get(tableName);
    }

    /**
     * @param keyspace
     * @param tableName
     * @param columnNames
     * @return
     */
    private String prepareQueryString(final String keyspace, final String tableName, final List<String> columnNames) {
        if (insertQueryStatement.get(tableName) == null) {
            System.out.println("Keyspace "+ keyspace);
            System.out.println("table name "+ tableName);
            System.out.println("columns "+ columnNames);
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
            LOGGER.info("InsertQueryStatement " + queryStringBuilder.toString());
            insertQueryStatement.put(tableName, queryStringBuilder.toString());
        }
        return insertQueryStatement.get(tableName);
    }
}
