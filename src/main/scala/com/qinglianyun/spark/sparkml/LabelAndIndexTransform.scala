package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:31 2018/12/13
  * @ 标签和索引转换
  * Spark机器学习中，经常需要将标签数据（一般是字符串）转化成整数索引，而在计算结束后又需要把整数索引还原为标签。
  * 这就涉及到几个转化器：StringIndexer、IndexToString、OneHotEncoder、VectorIndexer
  *
  * StringIndexer
  * StringIndexer是指把一组字符型标签编码成一组标签索引，索引的范围为0到标签数量，索引构建的顺序为标签的频率，
  * 优先编码频率较大的标签，所以出现频率最高的标签为0号。
  * 如果输入的是数值型的，我们会把它转化成字符型，然后再对其进行编码。
  * 在pipeline组件，比如Estimator和Transformer中，想要用到字符串索引的标签的话，我们一般需要通过setInputCol来设置输入列。
  * 另外，有的时候我们通过一个数据集构建了一个StringIndexer，然后准备把它应用到另一个数据集上的时候，
  * 会遇到新数据集中有一些没有在前一个数据集中出现的标签，这时候一般有两种策略来处理：
  * 第一种是抛出一个异常（默认情况下），第二种是通过掉用 setHandleInvalid("skip")来彻底忽略包含这类标签的行。
  *
  *
  * IndexToString
  * IndexToString的作用是把标签索引的一列重新映射回原有的字符型标签。
  * 一般都是和StringIndexer配合，先用StringIndexer转化成标签索引，进行模型训练，然后在预测标签的时候再把标签索引转化成原有的字符标签。
  * 当然，也允许你使用自己提供的标签。
  *
  * OneHotEncoder
  * 独热编码是指把一列标签索引映射成一列二进制数组，且最多的时候只有一位有效。这种编码适合一些期望类别特征为连续特征的算法，比如说逻辑斯蒂回归。
  *
  * VectorIndexer
  * VectorIndexer解决向量数据集中的类别特征索引。它可以自动识别哪些特征是类别型的，并且将原始值转换为类别索引。它的处理流程如下：
  * ​ 1.获得一个向量类型的输入以及maxCategories参数。
  * ​ 2.基于不同特征值的数量来识别哪些特征需要被类别化，其中最多maxCategories个特征需要被类别化。
  * ​ 3.对于每一个类别特征计算0-based（从0开始）类别索引。
  * ​ 4.对类别特征进行索引然后将原始特征值转换为索引。
  */
object LabelAndIndexTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val df1: DataFrame = sqlContext.createDataFrame(Seq(
      (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val model1: StringIndexerModel = indexer.fit(df1)
    val indexed1: DataFrame = model1.transform(df1)
    indexed1.show()

    val df2: DataFrame = sqlContext.createDataFrame(Seq(
      (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "d"))
    ).toDF("id", "category")

    val indexed2: DataFrame = model1
      .setHandleInvalid("skip") // 在新的数据集上出现新的标签时，处理策略
      .transform(df2) // 将标签应用到另一个数据集上
    indexed2.show()
    // #################################################################################################################

    val converter: IndexToString = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")
    // 将标签索引转换为原有的字符标签
    val converted: DataFrame = converter.transform(indexed1)
    converted.show()
    // #################################################################################################################

    val df3 = sqlContext.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c"),
      (6, "d"),
      (7, "d"),
      (8, "d"),
      (9, "d"),
      (10, "e"),
      (11, "e"),
      (12, "e"),
      (13, "e"),
      (14, "e")
    )).toDF("id", "category")
    val model3: StringIndexerModel = indexer.fit(df3)
    val indexed3: DataFrame = model3.transform(df3)
    val oneHotEncoder: OneHotEncoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val indexed4: DataFrame = oneHotEncoder.transform(indexed3)

    indexed4.show()
    // +---+--------+-------------+-------------+
    // | id|category|categoryIndex|  categoryVec|
    // +---+--------+-------------+-------------+
    // |  0|       a|          2.0|(4,[2],[1.0])|
    // |  1|       b|          4.0|    (4,[],[])|
    // |  2|       c|          3.0|(4,[3],[1.0])|
    // |  3|       a|          2.0|(4,[2],[1.0])|
    // |  4|       a|          2.0|(4,[2],[1.0])|
    // |  5|       c|          3.0|(4,[3],[1.0])|
    // |  6|       d|          1.0|(4,[1],[1.0])|
    // |  7|       d|          1.0|(4,[1],[1.0])|
    // |  8|       d|          1.0|(4,[1],[1.0])|
    // |  9|       d|          1.0|(4,[1],[1.0])|
    // | 10|       e|          0.0|(4,[0],[1.0])|
    // | 11|       e|          0.0|(4,[0],[1.0])|
    // | 12|       e|          0.0|(4,[0],[1.0])|
    // | 13|       e|          0.0|(4,[0],[1.0])|
    // | 14|       e|          0.0|(4,[0],[1.0])|
    // +---+--------+-------------+-------------+
    // 在上例中，我们构建了一个dataframe，包含“a”，“b”，“c”，“d”，“e” 五个标签，
    // 通过调用OneHotEncoder，我们发现出现频率最高的标签“e”被编码成第0位为1，即第0位有效，
    // 出现频率第二高的标签“d”被编码成第1位有效，依次类推，“a”和“c”也被相继编码，
    // 出现频率最小的标签“b”被编码成全0。
    // #################################################################################################################

    val data = Seq(
      Vectors.dense(-1.0, 1.0, 1.0),
      Vectors.dense(-1.0, 3.0, 1.0),
      Vectors.dense(0.0, 5.0, 1.0)
    )

    val df: DataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val vectorIndexer: VectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(2) // 设置最多有多少个特征被类别化
    val model5: VectorIndexerModel = vectorIndexer.fit(df)
    val categoricalFeatures: Set[Int] = model5.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))

    val indexed6: DataFrame = model5.transform(df)
    indexed6.foreach(println(_))

    spark.close()
  }
}
