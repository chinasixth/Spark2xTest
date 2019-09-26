import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object FlowKMeans {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("FlowLogisticRegress")
      .master("local[*]")
      .getOrCreate()

    val lineDF: DataFrame = spark.read
      .format("text")
      .load("src/main/data/flow/flow_detail.txt")
      .toDF("line")

    import spark.implicits._
    val fieldsDF: DataFrame = lineDF.map { line: Row =>
      val fields: Array[String] = line.getString(0).split(" ")

      val mac: String = fields(0)
      val srcIP: String = fields(1)
      val destIP: String = fields(2)
      val srcPort: String = fields(3)
      val destPort: String = fields(4)
      val packageNum: String = fields(5)
      val bytes: String = fields(6)
      val types: String = fields(7)
      val direction: String = fields(8)
      val time = fields(9) + " " + fields(10)

      destIP match {
        case "192.168.7.101" =>
          (mac, srcIP, destIP, srcPort.toInt, destPort.toInt, packageNum.toInt, bytes.toInt, types.toInt, direction.toInt, time, 0, 1)
        case _ =>
          (mac, srcIP, destIP, srcPort.toInt, destPort.toInt, packageNum.toInt, bytes.toInt, types.toInt, direction.toInt, time, 1, 0)
      }
    }.toDF("mac", "srcIP", "destIP", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "time", "label", "destIpIndexer")

    val srcIpIndexer = new StringIndexer()

    val srcIpModel: StringIndexerModel = srcIpIndexer
      .setInputCol("srcIP")
      .setOutputCol("srcIPIndexer")
      .fit(fieldsDF)

    val srcIpIndexerDF: DataFrame = srcIpModel.transform(fieldsDF)

    //    val destIpIndexer: StringIndexerModel = srcIpIndexer
    //      .setInputCol("destIP")
    //      .setOutputCol("destIpIndexer")
    //      .fit(srcIpIndexerDF)
    //
    //    val destIpDF: DataFrame = destIpIndexer.transform(srcIpIndexerDF)

    val macModel: StringIndexerModel = srcIpIndexer
      .setInputCol("mac")
      .setOutputCol("macIndexer")
      .fit(srcIpIndexerDF)

    val macDF: DataFrame = macModel.transform(srcIpIndexerDF)

    val featuresDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("macIndexer", "srcIPIndexer", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "destIpIndexer"))
      .setOutputCol("chooseFeatures")
      .transform(macDF)

    val pcaDF: DataFrame = new PCA()
      .setInputCol("chooseFeatures")
      .setOutputCol("features")
      .setK(9)
      .fit(featuresDF)
      .transform(featuresDF)

    val norDF: DataFrame = new Normalizer()
      .setInputCol("features")
      .setOutputCol("norFeatures")
      .setP(2)
      .transform(pcaDF)

    val Array(training, test) = norDF.randomSplit(Array(0.7, 0.3))

    val model: KMeansModel = new KMeans()
      .setK(3)
      .setSeed(1L)
      .setFeaturesCol("norFeatures")
      .fit(training)

    val result: DataFrame = model.transform(test)

    result
      .select("mac", "srcIP", "destIP", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "label", "chooseFeatures", "prediction")
      .where("label == prediction and destIP = \"192.168.7.101\"")
      .show(1000, false)

    result
      .select("mac", "srcIP", "destIP", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "label", "chooseFeatures", "prediction")
      .where("label != prediction")
      .show(1000, false)

    //    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
    //      .setLabelCol("label")
    //      .setPredictionCol("prediction")

    //    val accuracy: Double = evaluator.setMetricName("accuracy").evaluate(result)
    //    println(s"accuracy: ${accuracy}")
    //    val recall: Double = evaluator.setMetricName("weightedRecall").evaluate(result)
    //    println(s"recall: ${recall}")
    //    evaluator.setMetricName("f1") // 默认是f1
    //    val f1: Double = evaluator.evaluate(result)
    //    println(s"f1: $f1")

    val evaluator = new ClusteringEvaluator()

    val silhouette: Double = evaluator.evaluate(result)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    spark.close()
  }

}
