package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:08 2019/1/18
  * @ desc: SQLTransformer实现由SQL语句定义的转换。
  * 即，通过SQLTransformer可以用SQL语句操作DataFrame中的列，
  * 到spark-2.4.0时仅支持“SELECT …… FROM __THIS__ ……” WHERE “__THIS__”,
  * 其中“__THIS__”表示输入数据集的基础表，select子句指定要在输出中显示的字段、常量和表达式，并且可以是Spark SQL支持的任何select子句。
  * 用户还可以使用SparkSQl内置的函数和UDF对这些选定列进行操作。
  */
object SQLTransformerTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 1.0, 3.0),
      (2, 2.0, 5.0)
    )).toDF("id", "v1", "v2")
    // +---+---+---+
    // |id |v1 |v2 |
    // +---+---+---+
    // |0  |1.0|3.0|
    // |2  |2.0|5.0|
    // +---+---+---+

    val sQLTransformer: SQLTransformer = new SQLTransformer()
      .setStatement("SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    val sQLTransformered: DataFrame = sQLTransformer.transform(dataset)

    sQLTransformered.show(false)
    // +---+---+---+---+----+
    // |id |v1 |v2 |v3 |v4  |
    // +---+---+---+---+----+
    // |0  |1.0|3.0|4.0|3.0 |
    // |2  |2.0|5.0|7.0|10.0|
    // +---+---+---+---+----+

    spark.close()
  }

}
