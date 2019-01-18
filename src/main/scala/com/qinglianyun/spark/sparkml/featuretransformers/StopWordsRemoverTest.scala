package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 15:20 2019/1/8
  * @ desc: 删除停用词
  * stop word是一些经常出现但是没有什么含义的词，比如：the、a 等
  * StopWordsRemove将字符串序列（比如）作为输入，并从输入序列中删除所有停用词。
  *
  * 停用词列表由stopWords参数指定。其中某些语言的停用词可以通过调用StopWordRemover.loadDefaultStopWords(language)指定。如：english
  *
  */
object StopWordsRemoverTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    val stopWordsRemover: StopWordsRemover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("features")
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english")) // 默认是english，多次调用方法会覆盖

    val stopWordsRemovered: DataFrame = stopWordsRemover.transform(dataset)

    stopWordsRemovered.show(false)
    // +---+----------------------------+--------------------+
    // |id |raw                         |features            |
    // +---+----------------------------+--------------------+
    // |0  |[I, saw, the, red, balloon] |[saw, red, balloon] |
    // |1  |[Mary, had, a, little, lamb]|[Mary, little, lamb]|
    // +---+----------------------------+--------------------+

    spark.close()
  }

}
