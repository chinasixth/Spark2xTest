package com.qinglianyun.spark.sparkml.featureselectors

import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:14 2019/1/18
  * @ desc: VectorSlice是一个转换器，用于从向量列（列的类型是向量）中提取特征。
  * 它接收一个特征向量，并使用原始特征的子数组输出一个新的特征向量。
  *
  * VectorSlicer接受具有指定索引的向量列，然后输出一个新的向量列，它的值通过这些索引被选择。有两种指标：
  * 表示向量的索引的整数索引，setindexes()。
  * 表示向量中特征名称的字符串索引setNames()。
  * 这要求vector列具有AttributeGroup，因为实现与属性的name字段匹配。
  * 整数和字符串的规范都可以接受。此外，还可以同时使用整数索引和字符串名称。必须选择至少一个特性。不允许重复的特性，因此所选索引和名称之间不能有重叠。
  * 注意，如果选择了特性的名称，如果遇到空输入属性，将引发异常。
  * 输出向量将对具有所选索引的特性进行排序(按照给定的顺序)，然后是所选名称(按照给定的顺序)。
  *
  */
object VectorSlicerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()


    spark.close()
  }

}
