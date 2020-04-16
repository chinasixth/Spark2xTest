import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object CrossJoinTest {

  def main(args: Array[String]): Unit = {
    val d1 = Seq(
      (1, "nana", 18),
      (2, "cici", 19),
      (3, "juan", 17)
    )

    val d2 = Seq(
      (1, "nana", 18, "female"),
      (4, "nana", 18, "female")
    )

    val spark: SparkSession = SparkSession.builder()
      .appName("CrossJoin")
      .master("local[*]")
      .getOrCreate()

    val df1: DataFrame = spark.createDataFrame(d1)
    val df2: DataFrame = spark.createDataFrame(d2)
    df1.crossJoin(df2).show(false)

    spark.close()
  }

}
