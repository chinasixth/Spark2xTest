package com.qinglianyun.spark.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:02 2018/12/3
  * @ desc   ：实现每次程序启动时，都从kafka中offset为0的位置开始读取数据
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("StreamingWordCount")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node01:9092,node02:9092,node03:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "group_streaming_word_count",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // earliest  latest
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    // 可以同时指定多个topic，使用","分割
    val topics = Array("group_test", "test")

    //     手动指定offset的起始位置
    val fromOffsets = Map[TopicPartition, Long](
      new TopicPartition("test", 0) -> 0L,
      //      new TopicPartition("test", 1) -> 0L  // 手动设置消费的起始位置，需要正确指定TopicPartition，
      new TopicPartition("group_test", 0) -> 1L
    )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      // 设置数据缓存的方式
      LocationStrategies.PreferConsistent,
      // 指定消费topics和kafka参数
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets)
    )

    // 获取并存储offset到kafka
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(iter => {
        val o: OffsetRange = offsetRanges(TaskContext.get().partitionId())
        println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    val kvDS: DStream[(String, String)] = stream.map(record => (record.key(), record.value()))

    val linesDS: DStream[String] = kvDS.map(_._2)

    val words: DStream[String] = linesDS.flatMap(_.split(" "))

    val pairs: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
