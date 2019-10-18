package com.qinglianyun.spark.sparksql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object DatasetEncoderTest {
  private final val LOGGER = LoggerFactory.getLogger("DatasetEncoderTest")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DatasetEncoderTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data: Seq[String] = Seq("chen", "li", "huang")

    val value: Dataset[String] = spark.createDataset(data)

    /*
    * scala 中的编码器是自动推断的，如果是在java中，要手动指定编码器
    * */
    val hi: Dataset[String] = value.map(x => "hi," + x)

    val len: Dataset[Int] = hi.map(x => x.length)

//    implicit val test = Encoders.kryo[Person1]

    val persons: Dataset[Person1] = value.map(x => {
      val a: Int = x.length

      Person1(x, a)
    })

    persons.show()

    spark.close()


  }

}

case class Person1(name: String, age: Int)