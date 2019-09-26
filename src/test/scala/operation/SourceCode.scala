package operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object SourceCode {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SourceCode")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Seq("a", "b", "c", "d"), 3)

    println("rdd的分区数：" + rdd.partitions.length)

    /*
    * 第一个括号中的参数是初始值
    * 第二个括号中的第一个参数，是单个分区中的操作（初始值与分区中的第一个值操作；上次的整个操作结果再与当前分区中的第二个值进行操作）
    * 第二个括号中的第二个参数，是将上面操作的所有分区结果进行操作，具体步骤和分区内类似
    * */
    val str: String = rdd.aggregate("-")(_+_, _+_)

    println(str)

    sc.stop()
  }

}
