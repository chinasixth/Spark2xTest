package com.qinglianyun.spark.sparksql.datasource


import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object PhoenixRead {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("PhoenixTest")
      .master("local[*]")
      .getOrCreate()

    readHBaseByPhoenix1(spark)

    spark.close()
  }

  /**
    * 第一种方法
    * 'table' is the corresponding Phoenix table
    * 'columns' is a sequence of of columns to query
    * 'predicate' is a set of statements to go after a WHERE clause, e.g. "TID = 123"
    * 'zkUrl' is an optional Zookeeper URL to use to connect to Phoenix
    * 'conf' is a Hadoop Configuration object. If zkUrl is not set, the "hbase.zookeeper.quorum"
    * property will be used
    */
  def readHBaseByPhoenix1(spark: SparkSession): Unit = {
    val conf = new Configuration()
    val df: DataFrame = spark.sqlContext.phoenixTableAsDataFrame(
      "\"person\"",
      Seq("id", "name", "age", "address"),
      predicate = Some("\"name\"='cici'"),
      conf = conf,
      zkUrl = Some("ambari-test1:2181:/hbase-unsecure")
    )
    df.show(false)
    writeHBasePyPhoenix1(spark, df, conf = conf)
  }

  /**
    * 这种方式在新版本中测试未通过
    *
    * @param spark SparkSession
    */
  //  def readHBaseByPhoenix2(spark: SparkSession): Unit = {
  //    //    "org.apache.phoenix.spark"
  //    val df: DataFrame = spark.read
  //      .format("phoenix")
  //      .options(Map("table" -> "P", PhoenixDataSource.ZOOKEEPER_URL -> "ambari-test1:2181"))
  //      .load
  //    df.write.text("/user/spark/phoenix/")
  //  }

  /**
    * 将数据写出到HBase
    * 'table' is the corresponding Phoenix table
    * 'conf' is a Hadoop Configuration object. If zkUrl is not set, the "hbase.zookeeper.quorum" property will be used
    * 'zkUrl' is an optional Zookeeper URL to use to connect to Phoenix
    * 'skipNormalizingIdentifier' 是否使用DataFrame中的schema映射Column，如果使用默认将fieldName转换成大写
    *
    * @param spark SparkSession
    * @param df    DataFrame要写入HBase的数据
    * @param conf  Configuration
    */
  def writeHBasePyPhoenix1(spark: SparkSession, df: DataFrame, conf: Configuration): Unit = {
    df.saveToPhoenix("P",
      conf = conf,
      zkUrl = Some("ambari-test1:2181/hbase-unsecure"),
      skipNormalizingIdentifier = true
    )
  }
}
