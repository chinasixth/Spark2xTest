package com.qinglianyun.spark.structuredstreaming

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object StructuredKafkaWordCount {
  private final val LOGGER = LoggerFactory.getLogger("StructuredKafkaWordCount")


  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString


    val spark = SparkSession
      .builder()
      .appName("StructuredKafkaWordCount")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .getOrCreate()

    import spark.implicits._

    /*
    * subscribeType：subscribe subscribePattern assign({"topicA":[0,1],"topicB":[2,4]})
    *
    * kafka 数据源 schema
    * Column	        Type
    * key	            binary
    * value	          binary
    * topic	          string
    * partition	      int
    * offset	        long
    * timestamp	      long
    * timestampType	  int
    *
    * kafka source中可选配置：
    * startingOffsets：
    * endingOffsets：
    * failOnDataLoss：数据丢失是否导致整个程序失败
    * kafkaConsumer.pollTimeoutMs：轮询kafka超时时间
    * fetchOffset.numRetries：尝试抓取kafka offset次数
    * fetchOffset.retryIntervalMs：抓取kafka offset时间间隔
    * maxOffsetsPerTrigger：好像是每个触发间隔最大速率
    * minPartitions：
    *
    *
    * */
    val lines: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()

    val df: Dataset[(String, Int, Long, Timestamp, Int, String, String)] = lines
      .selectExpr("topic", "partition", "offset", "timestamp", "timestampType", "CAST(key as STRING)", "CAST(value as STRING)")
      .as[(String, Int, Long, Timestamp, Int, String, String)]

    val words: Dataset[(String, Int, Long, Timestamp, Int, String, String)] =
      df.flatMap { row: (String, Int, Long, Timestamp, Int, String, String) =>
        row._7.split(" ")
          .map((row._1, row._2, row._3, row._4, row._5, row._6, _))
      }

//    words.groupBy($"topic", $"partition", $"offset", $"timestamp", $"timestampType", $"key")

    val query: StreamingQuery = words
      .writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", checkpointLocation) // 用于故障恢复
      .start()

    query.awaitTermination()

    spark.close()

  }

}
