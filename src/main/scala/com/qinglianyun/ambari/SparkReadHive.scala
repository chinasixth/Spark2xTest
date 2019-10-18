package com.qinglianyun.ambari

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object SparkReadHive {
  private final val LOGGER = LoggerFactory.getLogger("SparkReadHive")

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkReadHive")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    val databases: DataFrame = spark.sql("show databases")

    databases.show(false)

    spark.close()

  }


}
