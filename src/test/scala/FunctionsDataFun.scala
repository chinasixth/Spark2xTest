import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object FunctionsDataFun {
  private final val URL = "jdbc:mysql://192.168.1.180:3306?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false"
  private final val USER = "root"
  private final val PASSWORD = "0Xqinglian_root"

  def main(args: Array[String]): Unit = {
    test()
  }

  def test(): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("FunctionsDataFun")
      .master("local[*]")
      .getOrCreate()

    val device: DataFrame = spark.read
      .format("jdbc")
      .option("url", URL)
      .option("dbtable", "iot.batch_device")
      .option("user", USER)
      .option("password", PASSWORD)
      .load()

    device.selectExpr(
      "id",
      "device_mac",
      "user_id",
      "product_id",
      "cast(role_id as Long) as role_id",
      "create_time",
      "to_date(create_time) as create_date",
      "hour(create_time) as create_hour"
    )
      .where("create_time = '2018-12-03 11:55:08'")
      .selectExpr("id as ID", "device_mac as deviceMac", "user_id as userId", "product_id as productID", "role_id", "create_time as createTime", "create_date as createDate", "create_hour as createHour")
      .union(
        device.selectExpr(
          "id",
          "device_mac",
          "user_id",
          "product_id",
          "cast(role_id as Long) as role_id",
          "create_time",
          "to_date(create_time) as create_date",
          "hour(create_time) as create_hour"
        )
          .where("create_time = '2017-05-25 14:32:10'")
      )
      .show(false)

    //      .printSchema()
    spark.close()
  }

}
