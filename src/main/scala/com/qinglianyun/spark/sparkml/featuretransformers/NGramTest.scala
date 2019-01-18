package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:30 2019/1/8
  * @ desc: n-gram是NLP（自然语言）处理中一个较为重要的语言模型，常用来做句子相似度比较，模糊查询，以及句子合理性，句子矫正等。
  * 对与n元模型，每个词都与它左边的最近的n-1个词有关联
  * 简单来说就是将单词转换成一个个连续词输出。
  *
  * 参考：https://www.cnblogs.com/gongxijun/p/9178879.html
  *
  * 将字符串的输入数组转换为n克数组的功能转换器。忽略输入数组中的空值。
  * 它返回一个n克数组，其中每个n克由一个空格分隔的单词字符串表示。
  * 当输入为空时，返回一个空数组。当输入数组长度小于n(每n克元素的数量)时，不返回n克。
  *
  */
object NGramTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat")
      )
    )).toDF("id", "sentence")

    val nGram: NGram = new NGram()
      .setInputCol("sentence")
      .setOutputCol("ngrams")
      .setN(6) // Default: 2。如果设置的特别长，那么将什么都不显示

    val nGrammed: DataFrame = nGram.transform(dataset)

    nGrammed.show(false)


    spark.close()
  }

}
