package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.VectorSizeHint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:26 2019/1/18
  * @ desc: VectorSizeHint用来显式指定向量的大小，且这种做法非常有用。
  * 例如，VectorAssembler使用其输入列中的大小信息来为其输出列生成大小信息和元数据。
  * 虽然在某些情况下，可以通过检查列的内容来获得此信息，但在DataFrame流中，内容在流启动之前不可用，
  * 也就是一开始并不能拿到Vector的大小，所以只有在元数据中提前人工指定向量的大小。
  * VectorSizeHint允许用户显式指定列的向量大小，以便VectorAssembler或其他可能需要知道向量大小的转换器可以使用该列作为输入。
  *
  * 要使用VectorSizeHint，用户需要设置inputCol和size参数，应用这个转换器可以根据DataFrame生成一个新的DataFrame，
  * 其中包含用户指定inputCol向量大小的更新元数据。对生成的DataFrame的下游操作可以使用metadata来获得这个向量的大小。
  *
  * VectorSizeHint还可以接收一个可选的handleInvalid参数，当向量列包含控制或大小错误的向量时，该参数控制其行为。
  * 默认情况下，handleInvalid设施为“error”，表示应该抛出异常。该参数也可以设置为“skip”，表示应该从生成的DataFrame中过滤掉无效的行；
  * 或者设置为“optimistic”，表示不检查该列中的无效值，应该保留所有行。请注意，使用“optimistic”可能会导致结果的DataFrame处于不一致的状态
  *
  */
object VectorSizeHintTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
      (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0)
    )).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    // +---+----+------+--------------+-------+
    // |id |hour|mobile|userFeatures  |clicked|
    // +---+----+------+--------------+-------+
    // |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
    // |0  |18  |1.0   |[0.0,10.0]    |0.0    |
    // +---+----+------+--------------+-------+

    val vectorSizeHint: VectorSizeHint = new VectorSizeHint()
      .setInputCol("userFeatures")
      .setSize(3)
      /*
      * 如何处理无效项的参数。无效的向量包括零和
      * 错误的大小。选项是' skip '(过滤掉含有无效向量的行)，' error '(抛出一个
      * 错误)和“乐观”(不要检查向量大小，并保留所有行)。默认“错误”。
      * 注意:用户在将此参数设置为“乐观”时要小心。使用
      * “乐观”选项将阻止转换器验证向量的大小
      * “inputCol”。列的元数据与其内容之间的不匹配可能导致
      * 使用该列时出现意外行为或错误。
      * */
      .setHandleInvalid("optimistic")

    val transformed: DataFrame = vectorSizeHint.transform(dataset)

    transformed.show(false)
    // setSize(3)，setHandleInvalid("skip")时，长度不等于3的将被跳过
    // +---+----+------+--------------+-------+
    // |id |hour|mobile|userFeatures  |clicked|
    // +---+----+------+--------------+-------+
    // |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
    // +---+----+------+--------------+-------+
    // setSize(2)，setHandleInvalid("skip")时，长度不等于2的将被跳过
    // +---+----+------+------------+-------+
    // |id |hour|mobile|userFeatures|clicked|
    // +---+----+------+------------+-------+
    // |0  |18  |1.0   |[0.0,10.0]  |0.0    |
    // +---+----+------+------------+-------+
    // setHandleInvalid("optimistic")时，长度不等于setSize的，也不做任何反应
    // +---+----+------+--------------+-------+
    // |id |hour|mobile|userFeatures  |clicked|
    // +---+----+------+--------------+-------+
    // |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
    // |0  |18  |1.0   |[0.0,10.0]    |0.0    |
    // +---+----+------+--------------+-------+

    spark.close()
  }

}
