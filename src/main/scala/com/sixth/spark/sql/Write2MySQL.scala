package com.sixth.spark.sql

import java.util.Properties

import org.apache.spark.sql._

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object Write2MySQL {

  /**
    * 完全覆盖表中的内容
    *
    * @param spark
    */
  def completeMode(spark: SparkSession): Unit = {
    import spark.implicits._
    val p: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306?useUnicode=true&characterEncoding=utf8")
      //      .option("query", "select name,age,sex,score,create_date,create_hour from p")
      .option("dbtable", "test1.p")
      .option("user", "root")
      .option("password", "123456")
      .load()

    //    p.printSchema()
    p.agg(functions.count("age")).printSchema()
    p.show(false)
    /*
    * 对于基本数据类型和case class，可以通过spark.implicits进行隐式转换，不用专门指定编码类型
    * 对于特殊类，如Row，需要手动指定编码器，指定方式如下
    * */
    implicit val test: Encoder[Row] = Encoders.kryo[Row]
    val value: Dataset[Row] = spark.createDataset(Seq(
      ("bb", 11, "男", 12, "2019-09-26", "12")
    )).as[Row]

    val cols: List[String] = "name" :: "age" :: "sex" :: "score" :: "create_date" :: "create_hour" :: Nil
    val ret: Dataset[Row] = p.selectExpr(
      "name",
      "age",
      "sex",
      "score",
      "create_date",
      "create_hour"
    ).where("name = 'haha'")
      .union(
        value
      )
    //      .toDF(cols: _*)

    /*
    * 结果写出到mysql，根据dataframe/dataset的列名进行映射
    * */
    val url = "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=utf8"
    val driver = "com.mysql.jdbc.Driver"
    val prop = new Properties()
    prop.setProperty("url", url)
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")

    //    ret.write
    //      .mode(SaveMode.Overwrite)
    //      .jdbc(url, "p", prop)
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Write2MySQL")
      .master("local[*]")
      .getOrCreate()

    completeMode(spark)

    spark.close()
  }

}
