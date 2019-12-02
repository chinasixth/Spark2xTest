package com.qinglianyun.spark.sparkstreaming

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:40 2019/2/13
  * @ desc: flume采集数据到kafka，通过spark streaming进行计算
  * 目的：测通flume -> kafka -> spark streaming，部署上线
  */
object WordCountFlume2Kafka {
  def main(args: Array[String]): Unit = {
    // 1.模板代码
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 设置序列化方式[rdd] [worker]
      .set("spark.rdd.compress", "true") // rdd压缩，占用空间比较小


    val ssc = new StreamingContext(conf, Seconds(5))

    // 2.接收参数
    val Array(bootstrapServer, groupId, topicList) = args

    // 3.处理参数
    val topics: Array[String] = topicList.split(",")

    // 4.准备KafkaParam
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServer,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // 如果没有初始偏移量，怎么办。
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      // 在kerberos环境下添加以下配置
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, // SASL_PLAINTEXT
      SaslConfigs.SASL_KERBEROS_SERVICE_NAME -> "kafka",
      SaslConfigs.SASL_MECHANISM -> SaslConfigs.GSSAPI_MECHANISM
    )

    // 5.如果有指定topic的offset需求，可以手动指定offset

    // 6.创建direct stream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 7.开始计算
    stream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {
      logger.info("####################程序开始运行####################")

      // 8.获取offset
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val valueRDD: RDD[String] = rdd.map((_: ConsumerRecord[String, String]).value())
      val splited: RDD[String] = valueRDD.flatMap((_: String).split(","))
      val mapped: RDD[(String, Int)] = splited.map((_: String, 1))
      val result: RDD[(String, Int)] = mapped.reduceByKey((_: Int) + (_: Int))
      result.foreach(println)

      // 9.存储offset到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    // 模板代码
    ssc.start()
    ssc.awaitTermination()

  }

}
