package com.qinglianyun.spark.sparkmllib

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:59 2018/12/7
  * @ 
  */
object CorrelationTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    //val data = Seq(
    //  Vectors.dense(1, 4),
    //  Vectors.dense(2, 5),
    //  Vectors.dense(3, 6)
    //)

    // toDF发生隐式转换
    import spark.implicits._
    val df: DataFrame = data.map(Tuple1.apply).toDF("features")
    df.select("features").show()
    val tuples: Seq[Tuple1[linalg.Vector]] = data.map(Tuple1.apply)
    println(s"tuples: \n${tuples}")

    // 第三个参数是指计算相似度的方法：pearson（默认）  spearman
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head()
    println(s"Pearson correlation matrix:\n ${coeff1}\n")


    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head()
    println(s"Spearman correlation matrix:\n ${coeff2}")

    // 测试鸢尾花数据
    val originalData: RDD[Array[String]] = spark.sparkContext.textFile("src/main/data/iris.data").map(_.split(","))
    val rowRDD: RDD[Row] = originalData.map(x => Row(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4)))
    val schema: StructType = StructType(
      Array(
        new StructField("el", DataTypes.DoubleType, false),
        new StructField("ew", DataTypes.DoubleType, false),
        new StructField("fl", DataTypes.DoubleType, false),
        new StructField("fw", DataTypes.DoubleType, false),
        new StructField("kind", DataTypes.StringType, false)
      )
    )
    val frame: DataFrame = spark.createDataFrame(rowRDD, schema)
    frame.show()




    spark.stop()
  }

}
