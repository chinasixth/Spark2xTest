package com.qinglianyun.spark.sparkml.featureextractors

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:39 2018/12/13
  * @ CountVectorizer和CountVectorizerModel旨在通过计数来将一个文档上转化为向量。
  * 和TF-IDF、Word2Vec类似，都是将文档向量化的算法。
  *
  * 词袋法：
  * 不考虑词语出现的顺序，每个出现过的词汇单独作为一列特征
  * 这些不重复的词汇集合为词表
  * 每一个文本都可以在很长的词表上统计出一个很多列的特征向量
  * 如果每个文本都出现的词汇，一般被标记为 停用词 不计入特征向量
  *
  */
object CountVectorizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    // 生成数据
    val df: DataFrame = sqlContext.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // 创建一个CountVectorizerModel。设定词汇表最大size为3，设定词汇表中的词至少要在2个文档中出现过
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3) // 词汇表最大容量
      .setMinDF(2) // 至少在两个文档中出现过
      .fit(df)
    cvModel.transform(df).select("features").foreach(println(_))
    // 从打印结果可以看到，词汇表中有“a”，“b”，“c”三个词，且这三个词都在2个文档中出现过。
    // 其中结果中前面的3代表的是vocabsize；“a”和“b”都出现了3次，而“c”出现两次，所以在结果中0和1代表“a”和“b”，2代表“c”；
    // 后面的数组是相应词语在各个文档中出现次数的统计。倘若把vocabsize设为2，则不会出现“c”。
    // 也可以用下面的方式来创建一个CountVectorizerModel，通过指定一个数组来预定义一个词汇表，在本例中即只包含“a”，“b”，“c”三个
    // [(3,[0,1,2],[1.0,1.0,1.0])]
    // [(3,[0,1,2],[2.0,2.0,1.0])]

    // 可以通过以下方式创建一个CountVectorizerModel，通过指定一个数组来预定义一个词汇表
    val model: CountVectorizerModel = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")
    println()
    model.transform(df).select("features").foreach(println(_))


    spark.close()
  }

}
