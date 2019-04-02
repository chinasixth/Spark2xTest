package com.qinglianyun.spark.sparksql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 18:54 2019/3/4
  * @ 
  */
object ParquetSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val parquetDF: DataFrame = spark.read
      .parquet("src/main/data/users.parquet")

    parquetDF.show(false)
    // +------+--------------+----------------+
    // |name  |favorite_color|favorite_numbers|
    // +------+--------------+----------------+
    // |Alyssa|null          |[3, 9, 15, 20]  |
    // |Ben   |red           |[]              |
    // +------+--------------+----------------+

    parquetDF.printSchema()
    // root
    //  |-- name: string (nullable = true)
    //  |-- favorite_color: string (nullable = true)
    //  |-- favorite_numbers: array (nullable = true)
    //  |    |-- element: integer (containsNull = true)

    parquetDF.createOrReplaceTempView("person_info")

    val result: DataFrame = spark.sql("SELECT name, favorite_color FROM person_info WHERE name = \"Ben\"")

    result.show(false)

    parquetDF.write
        .partitionBy("name")
        .parquet("src/main/data/parquet_dir")

    val par: DataFrame = spark.read
//      .option("basePath", "src/main/data/parquet_dir")
//      .parquet("src/main/data/parquet_dir/name=Ben")
      .parquet("src/main/data/parquet_dir/name=Ben")

    par.show(false)
    // 分区列将显示在表的最后面
    // +--------------+----------------+------+
    // |favorite_color|favorite_numbers|name  |
    // +--------------+----------------+------+
    // |null          |[3, 9, 15, 20]  |Alyssa|
    // |red           |[]              |Ben   |
    // +--------------+----------------+------+

    spark.close()
  }

}
