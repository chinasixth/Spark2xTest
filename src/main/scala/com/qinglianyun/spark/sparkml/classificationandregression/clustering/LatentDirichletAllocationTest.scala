package com.qinglianyun.spark.sparkml.classificationandregression.clustering

import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object LatentDirichletAllocationTest {

  /**
    * 官网实例
    *
    * @param spark
    */
  def latentDirichletAllocation(spark: SparkSession) = {

    val data = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_lda_libsvm_data.txt")

    val lda: LDA = new LDA()
      .setK(10)
      .setMaxIter(10)

    val model: LDAModel = lda.fit(data)

    val ll = model.logLikelihood(data)
    val lp = model.logPerplexity(data)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    val result: DataFrame = model.transform(data)
    result.show(false)

  }

  /**
    * 鸢尾花数据测试
    *
    * @param spark
    */
  def latentDirichletAllocationIris(spark: SparkSession) = {

    val data = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")

    import spark.implicits._
    val dataDF: DataFrame = data.map { x =>
      (x.getString(0).toDouble, x.getString(1).toDouble, x.getString(2).toDouble, x.getString(3).toDouble, x.getString(4))
    }.toDF("a", "b", "c", "d", "label")

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b", "c", "d"))
      .setOutputCol("features")

    val assemblerDF: DataFrame = assembler.transform(dataDF)

    val lda: LDA = new LDA()
      .setK(3)
      .setMaxIter(10)
      .setFeaturesCol("features")

    val model: LDAModel = lda.fit(assemblerDF)

    val result: DataFrame = model.transform(assemblerDF)

    result.show(150, false)

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    //    latentDirichletAllocation(spark)

    latentDirichletAllocationIris(spark)

    spark.close()
  }

}
