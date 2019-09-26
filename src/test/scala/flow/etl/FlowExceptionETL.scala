package flow.etl

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.slf4j.LoggerFactory

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in
  * @
  */
object FlowExceptionETL {
  final val logger = LoggerFactory.getLogger("FlowExceptionETL")

  final val timeInterval = 10000000 // 微秒

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FlowExceptionETL")
      .master("local[*]")
      .getOrCreate()

    val data: DataFrame = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("src/main/data/flow/exception/http_123_56_207_217_test5qinglianyuncom_30s_30n.pcap.result")
      .toDF("time", "msec", "pack_size", "smac", "dmac", "sip", "dip", "sport", "dport",
        "trans_protocol", "syn", "ack", "payload_len", "app_protocol", "http_host", "http_method", "http_uri")

    /*
   * 填充、替换、删除
   * */
    val dataset: DataFrame = data.select((data.col("time").cast(DataTypes.LongType) * 1000000 + data.col("msec")).as("timestamp"),
      data.col("pack_size"),
      data.col("smac"),
      data.col("dmac"),
      data.col("sip"),
      data.col("dip"),
      data.col("sport"),
      data.col("dport"),
      data.col("trans_protocol"),
      data.col("syn"),
      data.col("ack"),
      data.col("payload_len"),
      data.col("app_protocol"),
      data.col("http_host"),
      data.col("http_method"),
      data.col("http_uri")
    ).na.fill("isnull", Array("http_host", "http_method", "http_uri"))

    //    uriDF.select(
    //      uriDF.col("timestamp"),
    //      uriDF.col("pack_size"),
    //      uriDF.col("smac_label").as("smac"),
    //      uriDF.col("dmac_label").as("dmac"),
    //      uriDF.col("sip_label").as("sip"),
    //      uriDF.col("dip_label").alias("dip"),
    //      uriDF.col("sport"),
    //      uriDF.col("dport"),
    //      uriDF.col("trans_protocol"),
    //      uriDF.col("syn"),
    //      uriDF.col("ack"),
    //      uriDF.col("payload_len"),
    //      uriDF.col("http_host_label"),
    //      uriDF.col("http_method_label"),
    //      uriDF.col("http_uri_label")
    //    ).orderBy(uriDF.col("timestamp")) // 加上- 表示降序排序
    //      .coalesce(1)
    //      .write
    //      .option("header", "true")
    //      .mode(SaveMode.Overwrite)
    //      .csv("src/main/data/flow/exception/etl/http_etl.csv")

    val timeDF: DataFrame = dataset.orderBy(dataset.col("timestamp"))
      .select(
        dataset.col("timestamp"),
        dataset.col("timestamp").-(timeInterval).substr(0, 9).as("o_time"),
        dataset.col("pack_size"),
        dataset.col("smac").as("smac"),
        dataset.col("dmac").as("dmac"),
        dataset.col("sip").as("sip"),
        dataset.col("dip").alias("dip"),
        dataset.col("sport"),
        dataset.col("dport"),
        dataset.col("trans_protocol"),
        dataset.col("syn"),
        dataset.col("ack"),
        dataset.col("payload_len"),
        dataset.col("http_host"),
        dataset.col("http_method"),
        dataset.col("http_uri")
      )

    val lagDF: DataFrame = timeDF
      .select(
        timeDF.col("timestamp"),
        functions.lag(
          timeDF.col("timestamp"), 1, 0)
          .over(
            Window.partitionBy(
              timeDF.col("o_time"),
              timeDF.col("smac")
            )
              orderBy timeDF.col("timestamp")
          ).as("last_time"),
        timeDF.col("o_time"),
        timeDF.col("pack_size"),
        timeDF.col("smac"),
        timeDF.col("dmac"),
        timeDF.col("sip"),
        timeDF.col("dip"),
        timeDF.col("sport"),
        timeDF.col("dport"),
        timeDF.col("trans_protocol"),
        timeDF.col("syn"),
        timeDF.col("ack"),
        timeDF.col("payload_len"),
        timeDF.col("http_host"),
        timeDF.col("http_method"),
        timeDF.col("http_uri")
      )

    val intervalDF: DataFrame = lagDF
      .select(
        lagDF.col("timestamp"),
        (lagDF.col("timestamp") - lagDF.col("last_time")).as("interval"),
        lagDF.col("o_time"),
        lagDF.col("pack_size"),
        lagDF.col("smac"),
        lagDF.col("dmac"),
        lagDF.col("sip"),
        lagDF.col("dip"),
        lagDF.col("sport"),
        lagDF.col("dport"),
        lagDF.col("trans_protocol"),
        lagDF.col("syn"),
        lagDF.col("ack"),
        lagDF.col("payload_len"),
        lagDF.col("http_host"),
        lagDF.col("http_method"),
        lagDF.col("http_uri")
      )

    val meanDF: DataFrame = intervalDF.select(
      lagDF.col("timestamp"),
      intervalDF.col("interval"),
      intervalDF.col("o_time"),
      intervalDF.col("pack_size"),
      intervalDF.col("smac"),
      intervalDF.col("dmac"),
      intervalDF.col("sip"),
      intervalDF.col("dip"),
      intervalDF.col("sport"),
      intervalDF.col("dport"),
      intervalDF.col("trans_protocol"),
      intervalDF.col("syn"),
      intervalDF.col("ack"),
      intervalDF.col("payload_len"),
      intervalDF.col("http_host"),
      intervalDF.col("http_method"),
      intervalDF.col("http_uri"),
      functions.mean(intervalDF.col("interval")).over(Window.partitionBy(intervalDF.col("smac"),
        intervalDF.col("o_time")) orderBy intervalDF.col("interval")
        rangeBetween(Window.unboundedPreceding, Window.currentRow - 1)).cast(DataTypes.DoubleType).as("means")
    )

    intervalDF.select(
      functions.count(intervalDF.col("")) over(Window.rangeBetween(Window.currentRow - 10, Window.currentRow))
    )

    /*
    * 创建一个函数
    * */
    val handleInterval = (col1: Long, col2: Double, col3: Double) => {
      if (col1 > col2) {
        col3
      } else {
        col1
      }
    }

    val handle = functions.udf(handleInterval)
    spark.sparkContext.broadcast(handle)

    val fillDF: DataFrame = meanDF
      .na.fill(0.toDouble, Array("means"))

    val udfDF: DataFrame = fillDF
      .withColumn("zudf", handle(fillDF.col("interval"), fillDF.col("o_time"), fillDF.col("means")))

    udfDF.select(
      lagDF.col("timestamp"),
      udfDF.col("zudf").as("interval"),
      udfDF.col("o_time"),
      udfDF.col("pack_size"),
      udfDF.col("smac"),
      udfDF.col("dmac"),
      udfDF.col("sip"),
      udfDF.col("dip"),
      udfDF.col("sport"),
      udfDF.col("dport"),
      udfDF.col("trans_protocol"),
      udfDF.col("syn"),
      udfDF.col("ack"),
      udfDF.col("payload_len"),
      udfDF.col("http_host"),
      udfDF.col("http_method"),
      udfDF.col("http_uri")
    ).show(1000, false)


    spark.stop()

  }
}
