package com.qinglianyun.spark.structuredstreaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object NetworkWordCount {
  private final val LOGGER = LoggerFactory.getLogger("NetworkWordCount")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("NetworkWordCount")
      .enableHiveSupport()
      .getOrCreate()

    /*
    * socket kafka
    * */
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.1.181")
      .option("port", "9999")
      .load()

    import spark.implicits._
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    /*
    * groupBy 返回值类型为 RelationalGroupedDataset
    * 此处为 RelationalGroupedDataset 中的 count 方法
    * 区别于 DataFrame 中的 count
    * */
    val wordCounts: DataFrame = words.groupBy("value").count()

    /*
    * outputMode: 输出模式，取值有 append complete update
    * append：前提是已有的行不会发生更改，将最近的结果输出到接收器
    * update：会更改以后行，只将更新后的结果输出到接收器
    * */
    val query: StreamingQuery = wordCounts.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()

    spark.close()
  }

}
