package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object SQLTest {
  val data: Seq[String] = Seq(
    "A,2020-01,16",
    "A,2020-01,21",
    "A,2020-02,35",
    "A,2020-02,12",
    "B,2020-01,32",
    "B,2020-01,11",
    "B,2020-02,9",
    "B,2020-02,50"
  )

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger(this.getClass).setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("o").setLevel(Level.ERROR)
  Logger.getLogger("i").setLevel(Level.ERROR)
  Logger.getLogger("ch").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  Logger.getLogger("codegen").setLevel(Level.ERROR)
  Logger.getLogger("sqlline").setLevel(Level.ERROR)
  Logger.getLogger("tables").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    withJoinTest(data)
  }


  def withJoinTest(data: Seq[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SQL")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    val dataset: Dataset[String] = spark.createDataset(data)
    val month_salary: DataFrame = dataset.map((x: String) => {
      val fields: Array[String] = x.split(",")
      (fields(0), fields(1), fields(2))
    }).toDF("name", "salary_month", "salary")

    month_salary.createOrReplaceTempView("month_salary")
    //    spark.sql(
    //      """
    //        |SELECT A.name,
    //        |       A.salary_month,
    //        |       max(A.month_salary) max_salary,
    //        |       sum(B.month_salary) AS total_salary
    //        |FROM
    //        |  (SELECT name,
    //        |          salary_month,
    //        |          sum(salary) AS month_salary
    //        |   FROM month_salary
    //        |   GROUP BY name,
    //        |            salary_month) A
    //        |INNER JOIN
    //        |  (SELECT name,
    //        |          salary_month,
    //        |          sum(salary) AS month_salary
    //        |   FROM month_salary
    //        |   GROUP BY name,
    //        |            salary_month) B ON B.name=A.name
    //        |WHERE B.salary_month<=A.salary_month
    //        |GROUP BY A.name,
    //        |         A.salary_month
    //        |ORDER BY A.name,
    //        |         A.salary_month
    //      """.stripMargin)
    //      .show(false)


    spark.sql(
      """
        |SELECT name,
        |       salary_month,
        |       sum(salary) AS t_salary
        |FROM month_salary
        |GROUP BY name,
        |         salary_month WITH ROLLUP
      """.stripMargin).show(false)

    month_salary.rollup("name", "salary_month")
      .agg(Map(
        "salary" -> "sum"
      )).show(false)
    //    month_salary.as("A").joinWith(month_salary.as("B"), $"A.name" === $"B.name")
    //      .show(false)


    spark.close()
  }


}
