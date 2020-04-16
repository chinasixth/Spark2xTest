package com.qinglianyun.spark.structuredstreaming.watermark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object WatermarkBaseUpdate {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WatermarkBaseUpdate")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.1.221")
      .option("port", "9999")
      .load()

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val words = lines.as[String].map(s => {
      val arr: Array[String] = s.split(",")
      val date: Date = sdf.parse(arr(0))
      (new Timestamp(date.getTime), arr(1))
    }).toDF("ts", "word")

    val wordCounts: DataFrame = words
      .withWatermark("ts", "2 minutes") // 说明使用的时间戳是事件时间
      .groupBy(
      window($"ts", "10 minutes", "2 minutes"),
      $"word"
    )
      .count()

    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }

}
