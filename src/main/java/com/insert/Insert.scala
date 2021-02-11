//package com.insert
//
//import org.apache.spark.sql.DataFrame
//
//import scala.collection.JavaConversions._
//
//object Insert {
//
//  val insert = (df: DataFrame, keySpace: String, tableName: String) => {
//    val columnNames = df.columns.toList
//    CassandraDriverInsert.createConnection();
//    // testdf.rdd.foreach(row => driverInsert.insert("test", "spark_test1", columnNames,row.toSeq.asInstanceOf[Seq[Object]].toList, true))
//    df.foreach(row => CassandraDriverInsert.insert(keySpace, tableName, columnNames, row.toSeq.asInstanceOf[Seq[Object]].toList, true))
//  }
//}
