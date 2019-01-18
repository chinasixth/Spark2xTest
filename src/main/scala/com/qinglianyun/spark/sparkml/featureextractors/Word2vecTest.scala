package com.qinglianyun.spark.sparkml.featureextractors

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:06 2018/12/13
  * @ desc: 词向量
  *
  * Word2Vec算法是将词转换为向量
  * 即：作用就是将自然语言中的自此转为计算机可以理解的稠密向量。
  *
  * 在word2Vec出现以前，自然语言处理经常把自此转为离散的单独的符号，也就是Ont-Hot Encoder。
  *
  * 杭州 [0,0,0,0,0,0,0,1,0,……，0,0,0,0,0,0,0]
  * 上海 [0,0,0,0,1,0,0,0,0,……，0,0,0,0,0,0,0]
  * 宁波 [0,0,0,1,0,0,0,0,0,……，0,0,0,0,0,0,0]
  * 北京 [0,0,0,0,0,0,0,0,0,……，1,0,0,0,0,0,0]
  *
  * 在上面的这个例子中，四个词分别对应一个向量，向量中只有一个值为1，其余的都为0。
  * 但是，使用Ont-Hot Encoder有以下问题。一方面，城市编码是随机的，向量之间相互独立，看不出城市之间存在的关联关系。
  * 其次，向量维度的大小取决于词料库中字词的多少。
  * 如果将世界所有城市名称对应的向量合为一个矩阵的话，那么这个矩阵过于稀疏，并且会造成维度灾难。
  *
  * Word2Vec可以将Ont-Hot Encoder转化为低纬度的连续值，也就是稠密向量，并且其中意思相近的词将被映射到向量空间中相近的位置。
  *
  */
object Word2vecTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val documentDF: DataFrame = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text") // 注意apply后面不能加上小括号

    // 创建一个Word2vec并设置相应的参数，此处设置特征向量为3
    val word2Vec: Word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3) // 默认是5
      .setMinCount(0)

    // 读入训练集，调用fit方法生成一个Word2VctModel
    val model: Word2VecModel = word2Vec.fit(documentDF)

    // 利用Word2VecModel模型将文档转成特征向量
    val result: DataFrame = model.transform(documentDF)
    result.select("result")
      .take(3)
      .foreach(println(_))

    // 得到的特征向量可以应用到相关的机器学习算法中。
    result.show(false)
    // +------------------------------------------+----------------------------------------------------------------+
    // |text                                      |result                                                          |
    // +------------------------------------------+----------------------------------------------------------------+
    // |[Hi, I, heard, about, Spark]              |[-0.008142343163490296,0.02051363289356232,0.03255096450448036] |
    // |[I, wish, Java, could, use, case, classes]|[0.043090314205203734,0.035048123182994974,0.023512658663094044]|
    // |[Logistic, regression, models, are, neat] |[0.038572299480438235,-0.03250147425569594,-0.01552378609776497]|
    // +------------------------------------------+----------------------------------------------------------------+

    spark.close()
  }

}
