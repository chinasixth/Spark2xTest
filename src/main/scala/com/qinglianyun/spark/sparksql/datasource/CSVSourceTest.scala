package com.qinglianyun.spark.sparksql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:06 2019/3/4
  * @ 
  */
object CSVSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CSVSourceTest")
      .master("local[*]")
      .getOrCreate()

    val csvDF: DataFrame = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true") // 如果设置为true，那么读取数据时将自动推断数据的类型
      .option("header", "true") // 默认为false，如果不设置为true，那么表中第一行将作为数据，而不是列名
      .load("src/main/data/people.csv")

    csvDF.show(false)
    // +------+----+---------+
    // |  name| age|      job|
    // +------+----+---------+
    // | tuoer|  18|Developer|
    // |langer|  20|     tian|
    // |  momo|  22|     null|
    // |lanjie|null|   worker|
    // |  null|  26|  manager|
    // +------+----+---------+

    csvDF.printSchema()
    // 设置inferScheme为true的时候
    // root
    //  |-- name: string (nullable = true)
    //  |-- age: integer (nullable = true)
    //  |-- job: string (nullable = true)
    // 设置interSchema为false的时候
    // root
    //  |-- name: string (nullable = true)
    //  |-- age: string (nullable = true)
    //  |-- job: string (nullable = true)

    csvDF.createOrReplaceTempView("person")

    // 不支持update操作
    val result: DataFrame = spark.sql("select sum(age) from person where age is not null")

    result.show(false)


    spark.close()
  }

}
