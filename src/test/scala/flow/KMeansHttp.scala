package flow

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object KMeansHttp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KMeansHttp")
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/data/flow/exception/etl/http_etl.csv/part-00000-40650086-a08d-4da7-acb9-5699ef64885d-c000.csv")

    dataset.limit(10)
        .show(false)


    val stringIndexer = new StringIndexer()

    val smacDF: DataFrame = stringIndexer.setInputCol("smac")
      .setOutputCol("smac_label")
      .fit(dataset)
      .transform(dataset)

    val dmacDF: DataFrame = stringIndexer.setInputCol("dmac")
      .setOutputCol("dmac_label")
      .fit(smacDF)
      .transform(smacDF)

    val sipDF: DataFrame = stringIndexer.setInputCol("sip")
      .setOutputCol("sip_label")
      .fit(dmacDF)
      .transform(dmacDF)

    val dipDF: DataFrame = stringIndexer.setInputCol("dip")
      .setOutputCol("dip_label")
      .fit(sipDF)
      .transform(sipDF)

    val hostDF: DataFrame = stringIndexer.setInputCol("http_host")
      .setOutputCol("http_host_label")
      .fit(dipDF)
      .transform(dipDF)

    val methodDF: DataFrame = stringIndexer.setInputCol("http_method")
      .setOutputCol("http_method_label")
      .fit(hostDF)
      .transform(hostDF)

    val uriDF: DataFrame = stringIndexer.setInputCol("http_uri")
      .setOutputCol("http_uri_label")
      .fit(methodDF)
      .transform(methodDF)

    spark.close()


  }

}
