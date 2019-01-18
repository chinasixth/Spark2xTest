package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:34 2019/1/8
  * @ desc: 标记化是将一个文本分解为单个术语（通常是单词）的过程。
  *
  * Tokenizer可以实现标记化的功能。
  *
  * 根据源码中的解释：
  * A tokenizer that converts the input string to lowercase and then splits it by white spaces.
  * tokenizer有两个功能，一个是将字符串转换为小写，一个是根据空格将字符串划分。（先转换成小写，后划分）
  *
  * RegexTokenizer允许基于正则表达式匹配更高级的标记化，默认参数“pattern”的值是"\\s+"，用作分隔符以分割输入文本
  * 用户可以将参数"gaps"设置为false，指示正则表达式"pattern"不是分割间隙，而是将匹配的内容作为标记化的结果。
  *
  */
object TokenizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    val tokenized: DataFrame = tokenizer.transform(dataset)

    tokenized.show(false)
    // +---+-----------------------------------+------------------------------------------+
    // |id |sentence                           |words                                     |
    // +---+-----------------------------------+------------------------------------------+
    // |0  |Hi I heard about Spark             |[hi, i, heard, about, spark]              |
    // |1  |I wish Java could use case classes |[i, wish, java, could, use, case, classes]|
    // |2  |Logistic,regression,models,are,neat|[logistic,regression,models,are,neat]     |
    // +---+-----------------------------------+------------------------------------------+


    val regexTokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // \\W:非单词字符

    val regexTokenized: DataFrame = regexTokenizer.transform(dataset)

    regexTokenized.show(false)

    val RegexTokenizerGaps: RegexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\w+")
      .setGaps(false) // .setPattern中的参数不是分割，而是用于匹配，将匹配到的内容作为最终标记化的结果

    val RegexTokenizedGaps: DataFrame = RegexTokenizerGaps.transform(dataset)

    RegexTokenizedGaps.show(false)

    // 自定义一个函数
    val countTokens: UserDefinedFunction = udf { (words: Seq[String]) => words.length }

    // col()属于functions类中的方法
    // 附加一列，传入自定义函数，函数结果将作为新的一列进行展示
    RegexTokenizedGaps.withColumn("tokens", countTokens(col("words"))).show(false)

    spark.close()
  }

}
