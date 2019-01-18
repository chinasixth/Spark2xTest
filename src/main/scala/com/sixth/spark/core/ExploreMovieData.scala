package com.sixth.spark.core

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:03 2019/1/4
  * @ desc: 探索电影数据
  *
  * 当拿到数据以后，首先要做的，就是要清洗数据，以保证所有的数据都是可用的
  * 应当考虑到每一个字段可能出现
  *
  * 数据格式：
  * movieId|movieName(release_year)|release_date||url|0|0|0|0|0|0|0|0|1|0|0|0|0|0|1|0|0|0|0
  *
  * 5|Copycat (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0
  */
/*
* 统计电影年龄，即其发行年份到现在多少年了
* 统计方式：将发行年份作为key，value为1，然后reduceByKey
* 或者，先计算出每一个电影的年龄（当前年减去发行年），然后将年龄作为key，1作为value，进行count。
* 以上可以先分组再count，根据数据而定。
* */
object ExploreMovieData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val movieLines: RDD[String] = sc.textFile("src/main/data/ml-100k/u.item")
    println(movieLines.take(10).toBuffer)

    // 要拿到发行年份
    // 此时应该考虑到，如果数据中没有记录发行年份应该怎么办
    // 解决：如果没有发行时间，先给定一个发行时间为1900，然后将发行时间为1900的电影过滤掉
    // 1900年好像还没有电影
    val movieData: RDD[String] = movieLines.map(_.split("\\|\\|")(0))
    val years: RDD[Int] = movieData.map((data: String) => {
      val fields: Array[String] = data.split("\\|")
      if (fields.length == 3) {
        Integer.parseInt(fields(2).split("-")(2))
      } else {
        1900
      }
    })

    // 获取当前年
    val nowYear: Int = Calendar.getInstance().get(Calendar.YEAR)

    val movieAge: RDD[Int] = years.map(nowYear - _)

    println(movieAge.take(10).toBuffer)

    sc.stop()
  }

}
