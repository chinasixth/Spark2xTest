package com.sixth.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:33 2019/1/3
  * @ desc: 统计用户、行呗、职业和邮编的数目
  *
  * u.user数据格式：
  * userId|age|gender|occupation|zipCode
  */
object UserAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val userLines: RDD[String] = sc.textFile("src/main/data/ml-100k/u.user")

    /*
    * 因为数据量并不是很大，所以就没有进行cache操作
    * 当数据量较大，且RDD在后续计算过程中多次使用时，最好先进行cache，有效提高运算速度
    * */
    val userTuple5: RDD[(String, String, String, String, String)] = userLines.map(_.split("\\|"))
      .map(x => (x(0), x(1), x(2), x(3), x(4)))

    // 统计用户数目
    val userCount: Long = userTuple5.map {
      case (userId, age, gender, occupation, zipcode) => {
        userId
      }
    }.distinct().count()
    println(s"userCount: $userCount")

    // 统计性别数目
    val genderCount: Long = userTuple5.map(_._3).distinct().count()
    println(s"genderCount: $genderCount")

    // 统计职业
    val occupationCount: Long = userTuple5.map(_._4).distinct()
      .count()
    println(s"occupationCount：$occupationCount")

    // 统计邮编
    val zipcodeCount: Long = userTuple5.map(_._5).distinct().count()
    println(s"zipcodeCount: $zipcodeCount")

    sc.stop()
  }

}
