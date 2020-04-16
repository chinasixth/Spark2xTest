package com.qinglianyun.ambari

import com.qinglianyun.common.SparkConsts
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
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object KerberosKafkaWC {
  private final val logger: Logger = LoggerFactory.getLogger("KerberosKafkaWC")
  // kerberos conf
  val krb5Debug: String = "true"
  val krb5Path: String = "/etc/krb5.conf"
  val principal: String = "kafka001@HADOOP.COM"
  val keytab: String = "/etc/security/keytabs/kafka001.keytab"
  val kafkaKerberos: String = "/etc/kafka/conf/kafka_client_jaas.conf"

  def main(args: Array[String]): Unit = {
    //    import org.apache.log4j.{Logger, Level}
    //    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)
    // set global kerberos conf
    System.setProperty("java.security.krb5.conf", krb5Path)
    System.setProperty("sun.security.krb5.debug", krb5Debug)
    System.setProperty("java.security.auth.login.config", kafkaKerberos)

    val conf: SparkConf = new SparkConf()
      .setAppName("KerberosKafkaWC")
      .set(SparkConsts.SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", s"-Djava.security.auth.login.config=$kafkaKerberos")
      .set("spark.executor.extraJavaOptions", s"-Djava.security.auth.login.config=$kafkaKerberos")

    logger.info("args length is " + args.length)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    val Array(bootstrapServer: String, groupId: String, topicList: String) = args
    val topics: Array[String] = topicList.split(",")
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServer,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // 如果没有初始偏移量，怎么办。
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      // 在kerberos环境下添加以下配置
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SASL_PLAINTEXT", // SASL_PLAINTEXT
      SaslConfigs.SASL_KERBEROS_SERVICE_NAME -> "kafka",
      SaslConfigs.SASL_MECHANISM -> SaslConfigs.GSSAPI_MECHANISM
    )
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd: RDD[ConsumerRecord[String, String]] => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val result: RDD[(String, Int)] = rdd.flatMap((_: ConsumerRecord[String, String]).value().split(","))
        .map((_: String, 1))
        .reduceByKey((_: Int) + (_: Int))

      result.collect.foreach(println)

      // 9.存储offset到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
