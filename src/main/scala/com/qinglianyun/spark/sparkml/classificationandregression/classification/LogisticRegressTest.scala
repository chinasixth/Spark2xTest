package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, LogisticRegressionTrainingSummary}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object LogisticRegressTest {

  /**
    * 二分类逻辑回归
    *
    * @param spark
    */
  def binomialLogisticRegression(spark: SparkSession) = {
    val training: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    training.show(false)

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10) // 设置最大迭代次数，默认值：100
      .setRegParam(0.3) // 正则化参数的参数，默认值：0.0
      .setElasticNetParam(0.8) // 默认值：0.0，范围[0, 1]

    val lrModel: LogisticRegressionModel = lr.fit(training)

    // coefficients: 二项逻辑回归模型系数向量      intercept: 二项逻辑回归模型截距向量
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val result: DataFrame = lrModel.transform(training)

    result.show(false)


  }


  def multinomiaLogisticRegression(spark: SparkSession) = {
    val training: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    training.show(false)

    val mlr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setElasticNetParam(0.8)
      .setRegParam(0.3)
      .setFamily("multinomial")

    val mlrModel: LogisticRegressionModel = mlr.fit(training)

    val result: DataFrame = mlrModel.transform(training)

    result.show(false)


  }

  /**
    * 测试鸢尾花数据
    *
    * 使用LogisticRegression算法，需要制作label和features
    * label是所有维度的数据组成的向量，features 是标签
    *
    * @param spark
    */
  def multinomiaLogisticRegressionIris(spark: SparkSession) = {
    implicit val matchError = org.apache.spark.sql.Encoders

    // 模板代码读数据
    val irisData: DataFrame = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")
      .toDF("_0", "_1", "_2", "_3", "category")

    //    val colNames: Array[String] = irisData.schema.fieldNames
    //    colNames.map(f => irisData(f).cast(DoubleType))

    // 将标签转换成数字，然后才可以运算
    val label: DataFrame = spark.createDataFrame(Seq(
      (0, "Iris-setosa"),
      (1, "Iris-versicolor"),
      (2, "Iris-virginica")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")

    val indexerModel: StringIndexerModel = indexer.fit(label)

    val stringIndexerDF: DataFrame = indexerModel.transform(irisData)

    stringIndexerDF.show(false)

    // 将多列数据组成一个向量
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("_0", "_1", "_2", "_3"))
      .setOutputCol("features")

    // 转化DataFrame某列的数据类型
    val df: DataFrame = stringIndexerDF.withColumn("_0", new Column("_0").cast(DataTypes.DoubleType))
      .withColumn("_1", new Column("_1").cast(DataTypes.DoubleType))
      .withColumn("_2", new Column("_2").cast(DataTypes.DoubleType))
      .withColumn("_3", new Column("_3").cast(DataTypes.DoubleType))

    val assemblerDF: DataFrame = assembler.transform(df)

    // 切分数据集，将0.4作为训练集，0.6作为测试集
    val Array(train, test) = assemblerDF.randomSplit(Array(0.6, 0.4))

    val mlr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3) // 正则化参数（>=0）,主要用于控制残差范数与解的范数之间的相对大小
      .setElasticNetParam(0.8) // 弹性网络混合参数，范围[0,1]
      .setFamily("multinomial")

    val mlrModel: LogisticRegressionModel = mlr.fit(train.select("label", "features"))

    val result: DataFrame = mlrModel.transform(test.select("label", "features"))

    result.take(150).foreach((x: Row) => {
      println(x.mkString("[", ",", "]"))
    })

    // summary可以获取模型的一些信息，比如精确度
    val summary: LogisticRegressionTrainingSummary = mlrModel.summary
    println("accuracy: " + summary.accuracy)
  }

  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()


    //    binomialLogisticRegression(spark)

    //    multinomiaLogisticRegression(spark)

    multinomiaLogisticRegressionIris(spark)

    spark.close()
  }

}
