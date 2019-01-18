package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 15:45 2019/1/9
  * @ desc: 对应StringIndexer，IndexToString将一列标签索引映射回字符串的列
  *
  * 注意实现的步骤:
  * 1.加载数据集
  * 2.使用StringIndexer
  * 3.应用数据集得到StringIndexerModel模型
  * 4.使用模型转换数据集
  * 5.使用IndexToString
  * 6.转换利用StringIndexerModel模型得到的数据集
  *
  */
object IndexToStringTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val stringIndexerModel: StringIndexerModel = stringIndexer.fit(dataset)

    val stringIndexed: DataFrame = stringIndexerModel.transform(dataset)

    // 从模型中可以获得输入列名、输出列名
    println(s"Transformed string column '${stringIndexerModel.getInputCol}' " +
      s"to indexed column '${stringIndexerModel.getOutputCol}'")

    stringIndexed.show(false)

    val indexToString: IndexToString = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val indexToStringed: DataFrame = indexToString.transform(stringIndexed)

    indexToStringed.show(false)
    // +---+--------+-------------+----------------+
    // |id |category|categoryIndex|originalCategory|
    // +---+--------+-------------+----------------+
    // |0  |a       |0.0          |a               |
    // |1  |b       |2.0          |b               |
    // |2  |c       |1.0          |c               |
    // |3  |a       |0.0          |a               |
    // |4  |a       |0.0          |a               |
    // |5  |c       |1.0          |c               |
    // +---+--------+-------------+----------------+


    spark.close()
  }

}
