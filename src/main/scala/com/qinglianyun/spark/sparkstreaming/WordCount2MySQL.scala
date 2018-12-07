package com.qinglianyun.spark.sparkstreaming

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.qinglianyun.jdbc.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:22 2018/12/4
  * @ desc   : 从kafka中读取数据，进行word count操作，将最终的结果批量写出到MySQl
  * 步骤:
  *   1.Spark Streaming的模板代码
  *   2.接收参数并处理参数
  *   3.构建kafkaParams
  *   4.创建kafka stream
  *   5.获取offset并存储
  *   6.分析
  *   7.写出
  *
  * 注意：DStream中有一个或多个RDD，可以通过foreachRDD进行操作，而每一个RDD又是有一个或多个分区，所以可以嵌套foreachPartition
  */
object WordCount2MySQL {

  private val dataSource = new ComboPooledDataSource("qlcloud_c3p0")
  private val utils: JDBCUtils = JDBCUtils.getInstance()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      // 设置序列化方式， [rdd] [worker]
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 占用空间比较小
      .set("spark.rdd.compress", "true")
      .set("conf.setMaxSpoutPending", "true")
      .set("auto.commit.interval.ms", "1000")
      // .set("spark.streaming.kafka.maxRatePerPartition", "600")
      // .set("spark.hadoop.fs.oss.accessKeyId", "LTAIXyHKjhIcs2SQ")
      // .set("spark.hadoop.fs.oss.accessKeySecret", "1wxybpbq4WTuYta4ohBQ6J7kDCAPLA")
      // .set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.shuffle.consolidateFiles", "true")

    val ssc = new StreamingContext(conf, Seconds(5))

    val Array(bootStrapServers, groupId, topics) = args

    val topicsArr = topics.split(",")

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsArr, kafkaParams)
    )

    stream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(it => {
        val o: OffsetRange = offsetRanges(TaskContext.getPartitionId())
        println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      // TODO 业务逻辑
      val parRDD: RDD[ConsumerRecord[String, String]] = rdd.repartition(4)
      val result: RDD[(String, Int)] = parRDD.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//      result.foreachPartition((it: Iterator[(String, Int)]) => {
//        val sql = "insert into person values(?, ?)"
//        val params = new ArrayBuffer[Array[Any]]()
//        while (it.hasNext) {
//          val tuple: (String, Int) = it.next()
//          params.+=(Array(tuple._1, tuple._2))
//        }
//        val i: Int = utils.insertBatch(sql, params)
//        println("=============" + i)
//      })

      result.foreachPartition(it => {
        val sql = "delete from person where name = ? and age = ?"
        val params = new ArrayBuffer[Array[Any]]()
        while (it.hasNext){
          val tuple: (String, Int) = it.next()
          params.+=(Array(tuple._1, tuple._2))
        }
        utils.deleteBatch(sql, params)
        println("删除完成。。。")
      })

    })

    // TODO 业务逻辑

    ssc.start()
    ssc.awaitTermination()

  }

}
