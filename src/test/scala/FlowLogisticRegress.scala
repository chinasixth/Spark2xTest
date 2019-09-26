import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, LogisticRegressionTrainingSummary}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object FlowLogisticRegress {

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

    val regression = new LogisticRegression()
      .setMaxIter(10)
      //      .setRegParam(0.3)
      //      .setElasticNetParam(0.8)
      .setFeaturesCol("norFeatures")
      .setLabelCol("label")
      .setFamily("binomial")

    val model: LogisticRegressionModel = regression.fit(training.select("norFeatures", "label"))

    val resultDF: DataFrame = model.transform(test)

    val indexToString = new IndexToString()

    val macRs: DataFrame = indexToString
      .setInputCol("macIndexer")
      .setOutputCol("macCategory")
      .transform(resultDF)

    //    val destRs: DataFrame = indexToString
    //      .setInputCol("destIpIndexer")
    //      .setOutputCol("destIpCategory")
    //      .transform(macRs)

    val srcRs: DataFrame = indexToString
      .setInputCol("srcIPIndexer")
      .setOutputCol("srcIpCategory")
      .transform(macRs)

    //    srcRs
    //      .select("mac", "srcIP", "destIP", "srcPort", "destPort",o "packageNum", "bytes", "types", "direction", "label", "srcIPIndexer", "destIpIndexer", "macIndexer", "rawPrediction", "probability", "prediction", "macCategory", "srcIpCategory", "destIpCategory")
    //      .where("destIP = \"192.168.7.101\"")
    //      .show(2000, false)

    //    srcRs.show(1000, false)

    srcRs
      .select("mac", "srcIP", "destIP", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "label", "chooseFeatures", "prediction")
      .show(1000, false)

    srcRs
      .select("mac", "srcIP", "destIP", "srcPort", "destPort", "packageNum", "bytes", "types", "direction", "label", "chooseFeatures", "prediction")
      .where("label == prediction and destIP = \"192.168.7.101\"")
      .show(1000, false)
    //    srcRs
    //      .where("prediction = 0.0")
    //      .show(2000, false)

    // summary可以获取模型的一些信息，比如精确度
    val summary: LogisticRegressionTrainingSummary = model.summary
    println("accuracy: " + summary.accuracy)
    println("recall: " + summary.weightedRecall)
    println("f1: " + summary.weightedFMeasure)

    spark.close()

  }
}
