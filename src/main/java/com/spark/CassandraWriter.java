package com.spark;

import java.util.List;

//Call from Spark 
public class CassandraWriter {

    //public static
    static {
        System.out.println("Initializing WriterQueueConsumer #########");
        //WriterQueueConsumer consumer = new WriterQueueConsumer();
        Thread thread = new Thread(new WriterQueueConsumer());
        thread.start();
    }

    public static void insert(String keySpace, String tableName, List<String> columnNames, List<Object> columnValues) {
        WriterInfo obj = new WriterInfo(keySpace, tableName, columnNames, columnValues);
        //System.out.println("=====>" + columnValues.toString());

        try {
            WriterRepository.getQueue().put(obj);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println(obj.columnValues);
    }
}
