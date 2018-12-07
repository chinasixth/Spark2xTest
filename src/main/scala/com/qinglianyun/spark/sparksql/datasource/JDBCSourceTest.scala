package com.qinglianyun.spark.sparksql.datasource

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 12:07 2018/11/30
  * @ 
  */
object JDBCSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 第一种方式：
    //    val jdbcDF = spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306")
    //      .option("dbtable", "test.person")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .load()
    //
    //    jdbcDF.show()

    // 第二种方式：
    val connectProperties = new Properties()
    connectProperties.put("user", "root")
    connectProperties.put("password", "123456")
    //    val jdbcDF2 = spark.read
    //      .jdbc("jdbc:mysql://localhost:3306", "test.person", connectProperties)
    ////      .jdbc("jdbc:mysql://localhost:3306/test", "person", connectProperties)
    //    jdbcDF2.show()

    // 第三种方式
    //
    connectProperties.put("customSchema", "ages INT")
    val jdbcDF3 = spark.read
        .jdbc("jdbc:mysql://localhost:3306", "test.p", connectProperties)
    jdbcDF3.show()

    spark.close()
  }
}
