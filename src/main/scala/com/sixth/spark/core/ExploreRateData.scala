package com.sixth.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 15:18 2019/1/4
  * @ desc: 探索评级数据
  *
  * 对评分数据做一个简要的分析：总计、平均值、最值、中位数
  *
  * 数据格式：
  * userId|movieId|rating|timestamp
  */
object ExploreRateData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ratingLines: RDD[String] = sc.textFile("src/main/data/ml-100k/u.data")

    val ratings: RDD[Int] = ratingLines.map(_.split("\t")(2).toInt)

    val counter: StatCounter = ratings.stats()

    // stdev是标准偏差
    println(counter)

    sc.stop()
  }

}
