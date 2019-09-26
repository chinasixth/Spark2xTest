package ambarispark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark Sql")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()

    val ctlg: Catalog = spark.catalog

    ctlg.listDatabases().show(false)


    spark.close()


  }

}
