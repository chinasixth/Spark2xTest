package com.qinglianyun.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:41 2018/11/28
  * @ 
  */
object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("src/main/data/word.txt")

    val count: RDD[(String, Int)] = lines.rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    println(count.collect.toBuffer)

    spark.close()
  }

}
