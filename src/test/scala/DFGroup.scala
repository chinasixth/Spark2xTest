import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object DFGroup {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrame Group")
      .getOrCreate()
    val df: DataFrame = spark.createDataFrame(Seq(
      ("nana", 18, "女"),
      ("nana", 19, "女"),
      ("nana", 20, "女"),
      ("aa", 28, "男"),
      ("aa", 38, "男"),
      ("aa", 48, "男")
    )).toDF("name", "age", "sex")

    df.groupBy("name")
        .agg(
          functions.collect_list("sex").as("sex_list"),
          functions.sum("age").as("s_age")
        )
        .show()




    spark.close()
  }

}
