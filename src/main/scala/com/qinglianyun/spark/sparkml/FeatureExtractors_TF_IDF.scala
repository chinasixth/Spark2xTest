package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 9:54 2018/12/13
  * @ desc   : 主要测试和特征处理相关的算法，大体分为三类
  *
  * 特征抽取：从原始数据中抽取特征
  * 特征转换：特征的维度、特征的转化、特征的修改
  * 特征选取：从大规模特征集中选取一个子集
  *
  * 词频-逆向文件频率(TF-IDF)是一种在文本挖掘中广泛使用的特征向量化的方法，他可以体现一个文档中词语在语料库中的重要程度
  * TF: HashingTF是一个transformer，在文本处理中，接受词条的集合然后把这些集合转化成固定长度的特征向量。
  * 这个算法在hash的同时，会统计各个词条的词频
  * IDF：IDF是一个Estimator，会产生一个IDFModel
  * 使用步骤：
  * 1.创建集合，每一个句子代表一个文件。可以直接读文件，最终的形式要一样
  * 2.使用tokenizer将句子分解成单词
  * 3.使用HashingTF将句子哈希成特征向量
  * 4.new IDF并调用fit方法生成一个IDFModel
  *   可以条用transform方法，得到每一个单词对应的TF-IDF度量值
  *
  */
object FeatureExtractors_TF_IDF {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val sentenceData: DataFrame = sqlContext.createDataFrame(Seq(
      (0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    // 使用Tokenizer分解器，将句子划分为单词
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    // 通过transform方法查看效果
    val wordsData: DataFrame = tokenizer.transform(sentenceData)
    wordsData.foreach(println(_))
    // wordsData.select("words").show()

    // 用HashingTF的transform方法把句子哈希成特征向量。此处设置哈希表的桶数为2000
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(2000)

    // 通过transform查看效果
    val featureData: DataFrame = hashingTF.transform(wordsData)
    println()
    featureData.foreach(println(_))
    // 2000表示hash表的桶数，240表示对应word的哈希值，1.0表示对应word出现的次数
    // (2000,[240,333,1105,1329,1357,1777],[1.0,1.0,2.0,2.0,1.0,1.0])
    // featureData.show()

    // 调用IDF方法来重新构造特征向量的规模，生成的IDF是一个Estimator，在特征向量上应用它的fit方法，会产生一个IDFModel
    val idf: IDF = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val iDFModel: IDFModel = idf.fit(featureData)

    // 调用IDFModel的transform方法，可以获得每个单词对应的TF-IDF度量值
    // 度量值高的好一点。
    val rescaledData: DataFrame = iDFModel.transform(featureData)
    println()
    rescaledData.select("features","label")
        .take(3)
        .foreach(println(_))


    spark.close()
  }

}
