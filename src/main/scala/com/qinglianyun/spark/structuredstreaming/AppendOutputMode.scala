package com.qinglianyun.spark.structuredstreaming

import com.qinglianyun.common.SparkConsts
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object AppendOutputMode {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("AppendOutputMode")
      .master("local[*]")
      .config(SparkConsts.SPARK_SQL_SHUFFLE_PARTITIONS, 10)
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ambari-test1:4141")
      .option("subscribe", "tsingbox")
      .load()
    val data: Dataset[String] = df.selectExpr("CAST(value AS STRING)")
      .as[String]

    val origin: DataFrame = data.selectExpr("split(value, '\u0001')[0] as `event_time`",
      "split(value, '\u0001')[5] as `smac`",
      "split(value, '\u0001')[6] as `dmac`",
      "split(value, '\u0001')[7] as `sip`",
      "split(value, '\u0001')[8] as `dip`")

    val result: Dataset[(String, String, String, String)] = origin.selectExpr("event_time", "smac as mac", "sip as ip")
      .union(origin.selectExpr("event_time", "dmac as mac", "dip as ip"))
      .distinct()
      .groupBy("mac")
      .agg(
        functions.first("event_time").as("first_time"),
        functions.last("event_time").as("last_time"),
        functions.last("ip").as("ip")
      ).as[(String, String, String, String)]

    origin.selectExpr("event_time", "smac as mac", "sip as ip")
      .union(origin.selectExpr("event_time", "dmac as mac", "dip as ip"))
      .selectExpr("first(event_time) as first_time",
        "last(event_time) as last_time",
        "ip",
        "row_number() over(partition by mac order by event_time asc) as `number`"
      )

    val query: StreamingQuery = result
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("200 seconds"))
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "./checkpoint")
      .start()
    query.awaitTermination()

    spark.close()
  }

}
