package com.qinglianyun.spark.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:23 2019/3/26
  * @ 
  */
object DataSetAPITest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql._

    // 使用默认 Encoder，必不可少
    import spark.implicits._

    val jsonDF: DataFrame = spark
      .read
      // 如果一个 json串被格式化多行，必须指定下列属性
      .option("multiLine", "true")
      .json("src\\main\\data\\people.json")


    val jsonDS: Dataset[Person] = jsonDF.as[Person]

    //      .groupBy(jsonDS("name"))
    //      .agg(Map("age" -> "max"))

    val filterDS: Dataset[Person] = jsonDS.filter(_.name != null)

    filterDS.groupBy(filterDS("name"))
      .agg(Map("age" -> "max")).show()

    // 需要导入 org.apache.spark.sql.functions
    filterDS.groupBy(filterDS("name"))
      // agg 在 spark-1.4.0 之前默认不保留分组列。可以通过设置修改，具体见源码
      .agg(functions.max("age"))

    // cube groupBy 区别见： https://blog.csdn.net/u011622631/article/details/84786777
    filterDS.cube()

    spark.close()
  }

}

// 读取的 json 中的整数类型是 BigInt 类型，此处不能使用 Int
case class Person(name: String, age: BigInt)

//case class Person(name: String, age: Int)



