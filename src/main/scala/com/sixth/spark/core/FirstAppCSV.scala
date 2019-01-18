package com.sixth.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:02 2019/1/3
  * @ desc: 计算购买总次数、客户总个数、总收入
  */
object FirstAppCSV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val csvLines: RDD[String] = sc.textFile("src/main/data/userpruchasehistory.csv")

    val data: RDD[(String, String, String)] = csvLines.map(line => line.split(","))
      .map(x => (x(0), x(1), x(2)))

    // 计算购买总次数
    val count: Long = data.count()
    println(s"count: $count")

    // 计算有多少用户购买过商品
    val nameCount: Long = data.map {
      case (name, product, price) => {
        name
      }
    }.distinct().count()
    println(s"nameCount: $nameCount")

    // 计算总收入
    val totalPrice: Double = data.map {
      case (_, _, price) => {
        price.toDouble
      }
    }.sum()
    println(s"totalPrice: $totalPrice")

    // 求最畅销的产品
    val topOne: Array[(String, Int)] = data.map {
      case (_, product, _) => {
        (product, 1)
      }
    }.reduceByKey(_ + _).sortBy(_._2).top(1)
    println(s"topOne: ${topOne(0)._1}")

    sc.stop()
  }
}
