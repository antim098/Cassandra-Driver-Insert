package com.insert;

import com.datastax.driver.core.Session;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SaveToCassandra {
    public static void save(Dataset<Row> df, String keySpace, String tableName) {
        //CassandraDriverInsert.createConnection();
        List<String> columnNames = Arrays.asList(df.columns());
        //df.foreach((ForeachFunction<Row>) row -> CassandraDriverInsert.insert(keySpace, tableName, columnNames, (List<Object>) row.toSeq().toList(), true));
        df.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                Session session = CassandraConnector.connect();
                for (Iterator<Row> it = iterator; it.hasNext(); ) {
                    Row row = it.next();
                    CassandraDriverInsert.insert(keySpace, tableName, columnNames, JavaConverters.seqAsJavaList(row.toSeq()), session);
                    // do something with the row
                }
                CassandraConnector.closeSession(session);
            }
        });
    }
}