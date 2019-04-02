package com.qinglianyun.spark.sparkml.featureselectors

import java.util

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:14 2019/1/18
  * @ desc: VectorSlice是一个转换器，用于从向量列（列的类型是向量）中提取特征。
  * 它接收一个特征向量，并使用原始特征的子数组输出一个新的特征向量。
  *
  * VectorSlicer接受具有指定索引的向量列，然后输出一个新的向量列，它的值通过这些索引被选择。有两种指标：
  * 表示向量的索引的整数索引，setIndices()。
  * 表示向量中特征名称的字符串索引setNames()。
  * 这要求vector列具有AttributeGroup，因为实现与属性的name字段匹配。
  * 整数和字符串的规范都可以接受。此外，还可以同时使用整数索引和字符串名称。必须选择至少一个特性。不允许重复的特性，因此所选索引和名称之间不能有重叠。
  * 注意，如果选择了特性的名称，如果遇到空输入属性，将引发异常。
  * 输出向量将对具有所选索引的特性进行排序(按照给定的顺序)，然后是所选名称(按照给定的顺序)。
  *
  * 输入的是特征列，输出的是特征列的某几个特征组成的新的列。
  *
  */
object VectorSlicerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data: util.List[Row] = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    // 给每一维设置名字，后面可以通过名字选择哪些维度进行后续操作
    val defaultAttr: NumericAttribute = NumericAttribute.defaultAttr

    val attributes: Array[NumericAttribute] = Array("f1", "f2", "f3").map(defaultAttr.withName)

    val attributeGroup = new AttributeGroup("userFeatures", attributes.asInstanceOf[Array[Attribute]])

    val dataset: DataFrame = spark.createDataFrame(data, StructType(Array(attributeGroup.toStructField())))
    // 输入特征列
    // +--------------------+
    // |userFeatures        |
    // +--------------------+
    // |(3,[0,1],[-2.0,2.3])|
    // |[-2.0,2.3,0.0]      |
    // +--------------------+

    val vectorSlicer: VectorSlicer = new VectorSlicer()
      .setInputCol("userFeatures")
      .setOutputCol("features")
      .setIndices(Array(1)) // 选择第几维
      .setNames(Array("f3")) // 通过设置好的名字选择

    val vectorSlicered: DataFrame = vectorSlicer.transform(dataset)

    vectorSlicered.show(false)
    // 输出是特征列中的某几列组成的新列。
    // +--------------------+-------------+
    // |userFeatures        |features     |
    // +--------------------+-------------+
    // |(3,[0,1],[-2.0,2.3])|(2,[0],[2.3])|
    // |[-2.0,2.3,0.0]      |[2.3,0.0]    |
    // +--------------------+-------------+

    spark.close()
  }

}
