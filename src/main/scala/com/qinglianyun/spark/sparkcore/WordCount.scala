package com.qinglianyun.spark.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:33 2018/11/28
  * @ 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("src/main/data/word.txt")

    val count: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    println(count.collect.toBuffer)

    sc.stop()
  }

}
