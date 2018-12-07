package com.qinglianyun.spark.sparksql.datasource

import java.nio.file.{Files, Paths}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:40 2018/11/30
  * @ 
  */
object AvroSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/data/user.avsc")))
    val df = spark
      .readStream
      .format("kafka")
      .option(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092")
      .option("subscribe", "test")
      .load()

//    val output = df.select(from_avro())
  }

}
