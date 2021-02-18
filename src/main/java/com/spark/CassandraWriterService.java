package com.spark;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraWriterService implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(CassandraWriterService.class.getName());
    public static ConcurrentHashMap<String, PreparedStatement> preparedStatementMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> insertQueryStatement = new ConcurrentHashMap<>();
    WriterInfo writerInfo = null;
    String keyspace = null;
    String tableName = null;
    List<String> columnNames = null;
    List<List<Object>> rows = null;

    public CassandraWriterService(final String keyspace, final String tableName, final List<String> columnNames, List<List<Object>> rows) {
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.rows = rows;
    }

    public CassandraWriterService(WriterInfo writerInfo) {
        this.writerInfo = writerInfo;
    }

    @Override
    public void run() {
        if (this.rows != null && !this.rows.isEmpty()) {
            insertBatch(keyspace, tableName, columnNames, rows);
        } else {
            insert(writerInfo.getKeySpace(), writerInfo.getTableName(), writerInfo.getColumnNames(), writerInfo.getColumnValues());
        }
    }

    public void insertBatch(String keySpace, String tableName, List<String> columnNames, List<List<Object>> columnValues) {
        Session session = null;
        try {
            session = CassandraConnector.connect();
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            PreparedStatement prepared = getPreparedStatement(session, keySpace, tableName, columnNames);
            for (final List<Object> columnValue : columnValues) {
                BoundStatement bound = prepared.bind();
                batch.add(loadBoundStatement(columnNames, columnValue, bound));
                //session.executeAsync(loadBoundStatement(columnNames, columnValue, bound));
                WriterRepository.getCountUpAndDownLatch().countUp();
            }
            session.executeAsync(batch);

        } catch (Exception e) {
            LOGGER.info("[" + this + "] Exception occurred while trying to execute cassandra insert: " +
                    e.getMessage());
        } finally {
            //CassandraConnector.closeSession(session);
        }
    }

    public void insert(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues) {
        Session session = null;
        try {
            session = CassandraConnector.connect();
            PreparedStatement prepared = getPreparedStatement(session, keySpace, tableName, columnNames);
            BoundStatement bound = prepared.bind();
            session.executeAsync(loadBoundStatement(columnNames, columnValues, bound));
            WriterRepository.getCountUpAndDownLatch().countUp();
        } catch (Exception e) {
            System.err.println("[" + this + "] Exception occurred while trying to execute cassandra insert: " +
                    e.getMessage());
        } finally {
            //CassandraConnector.closeSession(session);
        }
    }

    private BoundStatement loadBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            Object value = columnValues.get(i);
            if (value != null && value != "") { // Skipping tombstones
                if (name.equals("raw_ts") || name.equals("ts")) {
                    boundStatement.setUUID(name,
                            UUID.fromString(value.toString()));
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
        }
        return boundStatement;
    }

    private PreparedStatement getPreparedStatement(final Session session, final String keyspace, final String tableName, final List<String> columnNames) {
        if (preparedStatementMap.get(tableName) == null) {
            preparedStatementMap.put(tableName, session.prepare(
                    prepareQueryString(keyspace, tableName, columnNames)));
        }
        return preparedStatementMap.get(tableName);
    }

    private String prepareQueryString(final String keyspace, final String tableName, final List<String> columnNames) {
        if (insertQueryStatement.get(tableName) == null) {
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
            System.out.println("InsertQueryStatement " + queryStringBuilder.toString());
            insertQueryStatement.put(tableName, queryStringBuilder.toString());
        }
        return insertQueryStatement.get(tableName);
    }

}
