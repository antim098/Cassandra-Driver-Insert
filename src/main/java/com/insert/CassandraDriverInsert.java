package com.insert;

import com.datastax.driver.core.*;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class CassandraDriverInsert implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(CassandraDriverInsert.class.getName());
    private static final Cluster cluster;
    private static final Session session;
    //public static List<BoundStatement> BoundStatementList = Collections.synchronizedList(new ArrayList<BoundStatement>());
    private static final List<BoundStatement> BoundStatementList = new CopyOnWriteArrayList<>();
    public static ConcurrentHashMap<String, PreparedStatement> preparedStatementMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> insertQueryStatement = new ConcurrentHashMap<>();
    public static Queue<BoundStatement> BoundStatementQueue = new ConcurrentLinkedQueue<BoundStatement>();
   // ConcurrentLinkedDeque<BoundStatement> testQueue = new ConcurrentLinkedDeque<BoundStatement>();
    private static Timer timer;
    private static ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    private static long timeMarker = 0;
    private static long processedRecords = 0;
    private static long failedRecords = 0;
    private static long batchEvaluationTime = Instant.now().toEpochMilli();

    static {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis(900000000);
        options.setReadTimeoutMillis(900000000);
        options.setTcpNoDelay(true);
        cluster = Cluster.builder()//.addContactPoints("10.105.22.171","10.105.22.172","10.105.22.173")
                .addContactPoints("localhost")
                .withPort(9042).withPoolingOptions(poolingOptions).withSocketOptions(options).build();
        LOGGER.info("Created Cluster Object" + cluster.getClusterName());
        session = cluster.connect();
        LOGGER.info("Created Session Object " + session.toString());
        if (timer == null) {
            timer = new Timer();
            timer.schedule(new java.util.TimerTask() {
                @Override
                public void run() {
                    executeBatchAsync(session);
                }
            }, 0, 3000);
        }
        LOGGER.info("Started Timer: " + timer);
    }


    public static long getProcessedRecordCount() {
        return processedRecords;
    }

    public static long getFailedRecordCount() {
        return failedRecords;
    }

    public static synchronized void executeBatchAsync(Session session) {
        //long size = BoundStatementList.size();
        if (BoundStatementList.size() >= 50000) {
            LOGGER.info("Current List Size : " + BoundStatementList.size());
            //batchEvaluationTime = Instant.now().toEpochMilli();
            synchronized (BoundStatementList) {
                BoundStatementQueue.addAll(BoundStatementList.subList(0, 50000));
                LOGGER.info("Queue Size : " + BoundStatementQueue.size());
                BoundStatementList.subList(0, 50000).clear();
            }
            LOGGER.info("List Size after clearing sublist: " + BoundStatementList.size());
            for (BoundStatement boundStatement : BoundStatementQueue) {
                session.executeAsync(boundStatement);
            }
            LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Records sent to execute : " + BoundStatementQueue.size());
            BoundStatementQueue.clear();
//            for (int i = 0; i < size; i++) {
//                session.executeAsync(BoundStatementList.get(i));
//            }

            //BoundStatementList.subList(0, (int) size).clear();
            //LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Processed 10,000 records");
        }
    }

    /**
     * @param keySpace
     * @param tableName
     * @param columnNames
     * @param columnValues
     */
    public void insert(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues) {
        //Session session = null;
        try {
//            if (session == null) {
//                CassandraConnector.connect(9042);
//                session = CassandraConnector.getSession();
//                //System.out.println(" Created session " + session.getState());
//            }
            //session = CassandraConnector.connect();
            //System.out.println("Got Session");
            //CassandraDriverInsert();
            //System.out.println("Column names "+ columnNames.toString());
            //System.out.println("Column Values "+columnValues.toString());
            //System.out.println("Generating prepared statement");
            PreparedStatement prepared = getPreparedStatement(session, keySpace, tableName, columnNames);
            //System.out.println("Prepared Statement Generated"+ prepared.getQueryString());
            BoundStatement bound = prepared.bind();
//            Futures.addCallback(session.executeAsync(loadIngestionBoundStatement(columnNames, columnValues, bound)), new FutureCallback<ResultSet>() {
//                @Override
//                public void onSuccess(ResultSet result) {
//                }
//
//                @Override
//                public void onFailure(Throwable t) {
//                    LOGGER.error(t.getMessage(), t);
//                }
//            });
            synchronized (BoundStatementList) {
                BoundStatementList.add(loadIngestionBoundStatement(columnNames, columnValues, bound));
            }
//            long timeElapsed = ((Instant.now().toEpochMilli() - batchEvaluationTime) / 1000);
//            if (timeElapsed > 5) {
//                LOGGER.info("Calling execute after :" + timeElapsed + " seconds");
//                batchEvaluationTime = Instant.now().toEpochMilli();
//                threadPoolExecutor.submit(new Runnable() {
//                    @Override
//                    public synchronized void run() {
//                        CassandraDriverInsert.this.executeBatchAsync(session);
//                    }
//                });
//            }
            //session.executeAsync(loadIngestionBoundStatement(columnNames, columnValues, bound));
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
//            if (processedRecords == 1000000) {
//                LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Processed all records : " + processedRecords);
//                CassandraConnector.closeSession(session);
//            }
            //CassandraConnector.closeSession(session);
        }
    }

    //    public synchronized void executeBatchAsync(Session session) {
//        if (BoundStatementList.size() == 10000) {
//            //CopyOnWriteArrayList<BoundStatement> executionList = new CopyOnWriteArrayList<>(BoundStatementList.subList(0, 10000));
//            //BoundStatementList.subList(0, 10000).clear();
//            for (int i = 0; i < 10000; i++) {
//                session.executeAsync(BoundStatementList.get(i));
//            }
//            batchEvaluationTime = Instant.now().toEpochMilli();
//            BoundStatementList.subList(0, 10000).clear();
//            LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Processed 10,000 records");
//        }
//    }


//    public static synchronized void executeBatchAsync(Session session) {
//        if (BoundStatementList.size() == 10000) {
//            CopyOnWriteArrayList<BoundStatement> executionList = new CopyOnWriteArrayList<>(BoundStatementList.subList(0, 10000));
//            BoundStatementList.subList(0, 10000).clear();
//            for (BoundStatement statement : executionList) {
//                session.executeAsync(statement);
//            }
//            LOGGER.info("[" + CassandraDriverInsert.class.getName() + "] Processed 10,000 records");
//        }
//    }

    /**
     * @param columnNames
     * @param columnValues
     * @param boundStatement
     * @return
     */
    public BoundStatement loadBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {
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
    public BoundStatement loadIngestionBoundStatement(List<String> columnNames, List<Object> columnValues, BoundStatement boundStatement) {

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
    public PreparedStatement getPreparedStatement(final Session session, final String keyspace, final String tableName, final List<String> columnNames) {
        //System.out.println("Inside prepared statement");
        if (preparedStatementMap.get(tableName) == null) {
            LOGGER.info("Creating New Prepared Statement " + "Session: " + session.toString() + "Cluster :" + session.getCluster().toString());
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
    public String prepareQueryString(final String keyspace, final String tableName, final List<String> columnNames) {
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
