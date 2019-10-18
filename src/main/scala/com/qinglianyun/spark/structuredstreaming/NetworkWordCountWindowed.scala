package com.qinglianyun.spark.structuredstreaming

import java.sql.Timestamp

import com.qinglianyun.common.SparkConfConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object NetworkWordCountWindowed {
  private final val LOGGER = LoggerFactory.getLogger("NetworkWordCountWindowed")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"


    val spark = SparkSession.builder()
      .appName("NetworkWordCountWindowed")
      .master("local[*]")
      .config(SparkConfConfig.SPARK_SQL_SHUFFLE_PARTITIONS, "10")
      .getOrCreate()

    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", "true")
      .load()

    /*
    * 调用 as[] 将无类型DataFrame转换成有类型的DataSet
    * */
    val words: DataFrame = lines.as[(String, Timestamp)]
      .flatMap((line: (String, Timestamp)) =>
        line._1.split(" ")
          .map(word => (word, line._2))
      ).toDF("word", "timestamp")

    /*
    * 设置水印时间，水印线就计算方式：上一批次的 max eventTime - watermark
    * */
    val windowedCounts: Dataset[Row] = words
      .withWatermark("timestamp", windowDuration)
      .groupBy(
        window($"timestamp", windowDuration, slideDuration), $"word"
      ).count().orderBy("window")

    val query: StreamingQuery = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()


    query.awaitTermination()

    spark.close()
  }

}
