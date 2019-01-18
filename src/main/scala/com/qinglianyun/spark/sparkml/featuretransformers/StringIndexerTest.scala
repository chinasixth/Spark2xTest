package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:40 2019/1/9
  * @ desc: StringIndexer，将标签字符串列编码为标签索引列
  * 将标签的字符串列映射到标签索引的ML列的标签索引器。
  * 如果输入列是数值型的，我们将其转换为字符串并索引字符串值。
  * 索引在[0,numtags]中。默认情况下，这是按标签频率排序的，因此最频繁的标签得到索引0。排序行为是通过设置' stringOrderType '来控制的。
  *
  * StringIndexer将标签字符串列编码为标签索引列时，索引是[0, numLabels)。
  * 支持四种排序选项：frequencyDesc、frequencyAsc、alphabetDesc、alphabetAsc
  * 默认是频率降序顺序。即出现次数最多的索引值为0
  *
  * 当在一个数据集上fit出StringIndexerModel模型，然后使用它来转换另一个数据集时，有三种策略来处理看不见的字符串（未在训练集中出现过的字符串）：
  * “error”： default，对于看不见的字符串报错
  * “skip”：对看不见的字符串跳过，即不进行索引
  * “keep”：对看不见的字符串保持，即将所有看不见的字符串的索引值统一设置为numLabels，也就是训练集中字符串种类总数
  *
  * 参考资料：https://blog.csdn.net/shenxiaoming77/article/details/63715525
  */
object StringIndexerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "features")

    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("features")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("skip")

    val stringIndexerModel: StringIndexerModel = stringIndexer.fit(dataset)

    val stringIndexed: DataFrame = stringIndexerModel.transform(dataset)

    stringIndexed.show(false)
    // +---+--------+-------------+
    // |id |features|categoryIndex|
    // +---+--------+-------------+
    // |0  |a       |0.0          |
    // |1  |b       |2.0          |
    // |2  |c       |1.0          |
    // |3  |a       |0.0          |
    // |4  |a       |0.0          |
    // |5  |c       |1.0          |
    // +---+--------+-------------+

    val test: DataFrame = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"))
    ).toDF("id", "features")

    val stringIndexed2: DataFrame = stringIndexerModel.transform(test)

    stringIndexed2.show(false)
    // 设置成keep，这里的3，就是训练集中不重复字符串的种类
    // +---+--------+-------------+
    // |id |features|categoryIndex|
    // +---+--------+-------------+
    // |0  |a       |0.0          |
    // |1  |b       |2.0          |
    // |2  |c       |1.0          |
    // |3  |d       |3.0          |
    // |4  |e       |3.0          |
    // +---+--------+-------------+

    // 设置成skip
    //  +---+--------+-------------+
    // |id |features|categoryIndex|
    // +---+--------+-------------+
    // |0  |a       |0.0          |
    // |1  |b       |2.0          |
    // |2  |c       |1.0          |
    // +---+--------+-------------+

    spark.close()
  }

}
