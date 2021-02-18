package com.spark;


import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class WriterQueueConsumer implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(WriterQueueConsumer.class.getName());

    static {
        LOGGER.info("Initializing cassandra connector #########");
        CassandraConnector.initialize("localhost", 9042, "cassandra", "cassandra", 120);
    }

    List<WriterInfo> list = new ArrayList<WriterInfo>();
    ExecutorService cassandraWriterService = Executors.newFixedThreadPool(1);

    @Override
    public void run() {

        try {
            while (true) {
                list.add(WriterRepository.getQueue().take());
                if (list.size() >= 1000) {
                    LOGGER.info("Got 1000 records");
                    //thread launch
                    boolean flag = true;
                    List<String> columnNames = null;
                    String keyspace = null;
                    String tableName = null;
                    List<List<Object>> rows = new ArrayList<>();
                    for (WriterInfo info : list) {
                        if (flag) {
                            columnNames = info.getColumnNames();
                            keyspace = info.getKeySpace();
                            tableName = info.getTableName();
                            flag = false;
                        }
                        rows.add(info.columnValues);
                    }

                    cassandraWriterService.submit(new CassandraWriterService(keyspace, tableName, columnNames, rows));
                    //clear list
                    list.clear();
                }

                //Insert each record
                //cassandraWriterService.submit(new CassandraWriterService(WriterRepository.getQueue().take()));

                if (WriterRepository.getCountUpAndDownLatch().getCount() > 0 && WriterRepository.getCountUpAndDownLatch().getCount() % 100000 == 0) {
                    LOGGER.info("processed " + WriterRepository.getCountUpAndDownLatch().getCount());
                }

            }
        } catch (Exception e) {
            Thread.currentThread().interrupt();
        }

    }
}
