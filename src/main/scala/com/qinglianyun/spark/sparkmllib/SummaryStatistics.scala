package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.stat.{KernelDensity, MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:33 2018/12/10
  * @ desc   ：基本的统计工具
  *
  * 给定一个数据集，数据分析一般会先观察一下数据集的基本情况，称之为汇总统计或者概要性统计。
  * 一般的概要性统计用于概括一系列的观察值，包括位置或集中趋势（比如算术平均值、中位数、众数、和四分位均值），
  * 展型（比如四分位间距，绝对偏差和绝对距离偏差、各阶矩），统计离差，分布的形状、依赖性等。
  * 除此之外，spark.mllib库也提供了一些其他的基本的统计分析工具，包括相关性、分层抽样、假设检验、随机数生成等。
  * 在此，将从以下几个方面测试spark mllib：
  * 概括性统计：summary statistics
  * 相关性：correlations
  * 假设检验：hypothesis testing
  * 随机数生成：random data generation
  * 核密度估计：Kernel density estimation
  *
  * 摘要统计：Summary statistics
  * 对于RDD[Vector]类型的变量，Spark MLlib提供了一种叫colStats()的统计方法，调用该方法会返回一个类型为MultivariateStatisticalSummary的实例。
  * 通过这个实例，可以获得每一列的最大值、最小值、均值、方差、总数等。具体操作间下方代码。
  *
  * 相关性（Correlations)
  * Correlations，相关度量，目前Spark支持两种相关系数：皮尔逊相关系数（pearson）和斯皮尔曼相关系数（spearman）。
  * 相关系数时用以反映变量之间相关关系密切成都的统计指标。简单的来说就是相关系数绝对值越大(值越接近±1)相关系数越高。
  * pearson相关系数表达的时两个数值变量的线性相关性，它一般适用于正态分布，取值范围为±1之间，0表示不相关。
  * spearman相关系数表达的时两个数值变量的相关性。没有pearson相关系数对变量要求那么严格。
  * 根据输入的类型不一样，输出的结果也会产生相应的变化，如果输入的是两个RDD[Double],则输出的是一个double类型的结果；
  * 如果输入的是一个RDD[Vector],则对应的输出结果是一个相关系数矩阵。
  *
  * 分层抽样（Stratified sampling）
  * 分层抽样，就是将数据根据不同的特征分成不同的组，然后按照特定条件从不同的组中获取样本，并重新组成新的数组。
  * 分层抽样算法是直接集成到键值对类型RDD[(K,V)]的sampleByKey和sampleByKeyExact方法，无需通过额外的spark.mllib库来支持。
  * （一）sampleByKey()方法
  * sampleByKey方法需要作用于一个键值对数组，其中的key用于分类，value可以是任意数。然后通过fractions参数来定义分类条件和采样机率，1.0代表100%
  * （二）sampleByKeyExact方法
  * sampleByKey和sampleByKeyExact方法的区别在于，每次都通过给定的概率以一种类似抛硬币的方式来决定这个观察值是否被放入样本，
  * 因此一遍就可以过滤完所有的数据，最后得到的一个近似大小的样本，但往往不够准确。
  * 而sampleByKeyExact会对全量数据做采样计算。
  * 对每个类别，其都会产生（fk·nk）个样本，其中fk是键为k的样本类别采样的比例；nk是键k所拥有的样本数。
  * sampleByKey采样的结果更准确，有99.99%的置信度，但耗费的计算资源也很多。
  *
  * 假设检验（Hypothesis testing）
  * Spark目前支持皮尔森卡方检测（Pearson's chi-squared tests），包括“适配度检定/拟合优度检验”（Goodness of fit）以及“独立性检定”（independence）
  * （一）拟合优度检验（Goodness of fit）
  * 回归直线对观察值的拟合程度（百度百科）。验证一组观察值的次数分配是否异于理论上的分配。
  * 其H0假设（虚无假设，null hypothesis）为一个样本中已发生事件的次数分配会服从某个特定的理论分配。
  * 实际执行多项式实验而得到的观察次数，与虚无假设的期望次数相比较，检验二者接近的程度，利用样本数据以检验总体分布是否为某一特定分布的统计方法。
  * 通常情况下，上面说的特定的理论分配一般是均匀分配。目前的Spark默认的也是均匀分配。
  * 拟合优度检验要求输入的是Vector，独立性检验要求输入的是Matrix
  * （二）独立性检验（Indenpendence）
  * 卡方独立性检测是用来检验两个属性间是否独立。其中一个属性作为行，另外一个作为列，通过貌似相关的关系考察其是否真实的存在相关性。
  * 独立性假设，也就是假设两个属性是否独立。当P值越大，越无法拒绝“两个属性无关”的假设。也就是P值越大，两个属性越可能无关。
  *
  * 随机数生成（Random data generation）
  * RandomRDDs是一个工具集，用来生成含有随机数的RDD，可以按各种戈丁的分布模式生成数据集，RandomRDDs包下现支持正态分布、泊松分布和均匀分布。
  * RandomRDDs提供随机double RDDs或Vector RDDs
  *
  * 核密度估计（Kernel density estimation）
  * Spark ML提供了一个工具类KernelDensity用于核密度估算，核密度估算的意思是根据已知的样本估计未知的密度，属于非参数检验方法之一。
  * 如果一个数在观察集中出现了，那么就说这个数的概率密度很大，离这个数近的概率密度也会很大，离这个数远的概率密度就会很小。
  * spark目前还支持一种，叫高斯核
  *
  *
  * 说明：使用的测试数据是鸢尾花的数据。前四列分别为花萼的长度、花萼的宽度、花瓣的长度、花瓣的宽度
  *
  */
object SummaryStatistics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 读取要分析的数据（以鸢尾花数据为例），把数据转换成RDD[Vector]类型
    val data: RDD[String] = sc.textFile("src/main/data/iris.data")
    val vectors: RDD[linalg.Vector] = data.map(_.split(",")).map((x: Array[String]) =>
      Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble))

    // 调用Statistics的colStats方法，得到MultivariateStatisticalSummary类的实例
    val summary: MultivariateStatisticalSummary = Statistics.colStats(vectors)
    // 调用统计方法，获得相应的结果
    println(s"列的大小（列总数）： ${summary.count}\n" +
      s"每列的均值： ${summary.mean}\n" +
      s"每列的方差： ${summary.variance}\n" +
      s"每列的最大值： ${summary.max}\n" +
      s"每列的最小值： ${summary.min}\n" +
      s"每列的L1范数： ${summary.normL1}\n" +
      s"每列的L2范数： ${summary.normL2}\n" +
      s"每列非零向量的个数： ${summary.numNonzeros}")
    // #################################################################################################################

    // 花萼的长度和花萼的宽度之间是否有关联性
    // 从数据中读取两个series，要求两个series的数据量必须相同
    val seriesX: RDD[Double] = sc.textFile("src/main/data/iris.data").map(_.split(",")).map(_ (0).toDouble)
    val seriesY: RDD[Double] = sc.textFile("src/main/data/iris.data").map(_.split(",")).map(_ (1).toDouble)
    // 调用Statistics中的corr函数来求两个数据集的相关性。
    // 默认调用的pearson
    val pearson: Double = Statistics.corr(seriesX, seriesY)
    println(s"pearson: $pearson")
    // 显示调用spearman
    val spearman: Double = Statistics.corr(seriesX, seriesY, "spearman")
    println(s"spearman: $spearman")

    // 求相关系数矩阵
    // 首先创建一个RDD[Vector]
    val vectorRDD: RDD[linalg.Vector] = sc.textFile("src/main/data/iris.data")
      .map(_.split(",")).map(x => Vectors.dense(x(0).toDouble, x(1).toDouble))
    val pearsonMatrix: Matrix = Statistics.corr(vectorRDD)
    println(s"pearsonMatrix:\n $pearsonMatrix")
    val spearmanMatrix = Statistics.corr(vectorRDD, "spearman")
    println(s"spearmanMatrix:\n $spearmanMatrix")
    // #################################################################################################################

    // 创建一组数据
    // makeRDD内部调用了parallelize方法
    val sexRDD: RDD[(String, String)] = sc.makeRDD(Array(
      ("female", "nv1"),
      ("female", "nv2"),
      ("female", "nv3"),
      ("female", "nv4"),
      ("female", "nv5"),
      ("male", "nan1"),
      ("male", "nan2"),
      ("male", "nan3"),
      ("male", "nan4"),
      ("male", "nan5")
    ))
    // 创建一个fractions，用来定义分类条件和采样几率
    val fractions: Map[String, Double] = Map[String, Double](
      "female" -> 0.6,
      "male" -> 0.4
    )
    // 开始采样
    // 参数的解释：
    // 第一个参数：每次抽样是否有放回
    // 第二个参数：分类条件和采样几率
    // 第三个参数：随机数种子。静态种子可以保证在配置不变的情况下，程序再次运行时抽样结果是不变化的；如果不指定成静态随机种子，那么每次运行的抽样结果是发生变化的。
    val approxSample: RDD[(String, String)] = sexRDD.sampleByKey(false, fractions, 1)
    // 因为数据量小，所以随机采样的结果并不能安全按照给定的采样机率进行采样。
    println(s"采样结果：\n${approxSample.collect.toBuffer}")
    // 读取两百条的数据，进行分层采样测试
    val sampleData: RDD[String] = sc.textFile("src/main/data/sample.txt")
    val sampleRDD: RDD[(String, String)] = sampleData.map(_.split(",")).map(x => (x(0), x(1)))
    // 采样
    val approxSample2: RDD[(String, String)] = sampleRDD.sampleByKey(false, fractions, 1)
    println(s"200条数据采样：\n${approxSample2.collect.toBuffer}")
    val sampleCount: RDD[(String, Int)] = approxSample2.map(x => (x._1, 1)).reduceByKey(_ + _)
    println(s"sampleCount: \n${sampleCount.collect.toBuffer}")

    // 使用sampleByKeyExact采样
    val exactSample: RDD[(String, String)] = sexRDD.sampleByKeyExact(false, fractions, 1)
    println(s"exactSample: \n${exactSample.collect.toBuffer}")
    val exactSampleRDD: RDD[(String, String)] = sampleRDD.sampleByKeyExact(false, fractions, 1)
    println(s"exactSampleRDD: \n${exactSampleRDD.collect.toBuffer}")
    val exactCount: RDD[(String, Int)] = exactSampleRDD.map(x => (x._1, 1)).reduceByKey(_ + _)
    println(s"exactCount: \n${exactCount.collect.toBuffer}")
    // #################################################################################################################

    // 从数据集中选择要分析的数据。取出iris.data中的前两条数据，不同的输入类型决定了是做拟合优度检验还是独立性检验
    // 拟合优度检验要求输入的是Vector，独立性检验要求输入的是Matrix
    val v1: linalg.Vector = sc.textFile("src/main/data/iris.data")
      .map(_.split(","))
      .map(x => Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble))
      .first()
    val v2: linalg.Vector = sc.textFile("src/main/data/iris.data")
      .map(_.split(","))
      .map(x => Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble))
      .take(2)
      .last
    val chiSqTestResult1: ChiSqTestResult = Statistics.chiSqTest(v1)
    /*
    * 简单解释每个输出的意义：
    * method：方法。这里采用的是pearson
    * statistic：检验统计量。见到那来说就是用来决定是否原假设的证据。检验统计量的值是利用样本数据计算得到的，
    * 它代表了样本中的信息。检验统计量的绝对值越大，拒绝原假设的理由越充分，反之，不拒绝原假设的理由越充分。
    * degrees of freedom：自由度。表示可自由变动的样本观测值的数目。
    * pValue：统计学根据显著性检验方法所得到P值，一般以P<0.05为显著，P<0.01为非常显著，
    * 其演绎是样本间的差异由抽样误差所致的概率小于0.05或0.01
    * 一般来说，假设检验主要看P值就可以了。
    * */
    println(s"chiSqTestResult1: \n${chiSqTestResult1}")
    // chiSqTestResult1:
    // Chi squared test summary:
    // method: pearson
    // degrees of freedom = 3
    // statistic = 5.588235294117647
    // pValue = 0.1334553914430291
    // #################################################################################################################

    // 通过v1，v2构造一个举证Matrix，然后进行独立性检验
    // 样本序号与数据值无关的假设
    val mat: linalg.Matrix = Matrices.dense(2, 2, Array(v1(0), v1(1), v2(0), v2(1)))
    println(s"mat: \n${mat}")
    val chiSqTestResult2: ChiSqTestResult = Statistics.chiSqTest(mat)
    println(s"chiSqTestResult2: ${chiSqTestResult2}")
    // mat:
    // 5.1  4.9
    // 3.5  3.0
    // chiSqTestResult2: Chi squared test summary:
    // method: pearson
    // degrees of freedom = 1
    // statistic = 0.012787584067389817
    // pValue = 0.90996538641943
    // #################################################################################################################

    // 将v1作为样本，把v2作为期望值
    val c1: ChiSqTestResult = Statistics.chiSqTest(v1, v2)
    // 结果可以看出样本v1与期望值等于v2的数据分布并无显著差异。事实上，v1和v2很像，v1可以看做是从期望值为v2的数据中抽样出来的。
    println(s"c1: ${c1}")
    // method: pearson
    // degrees of freedom = 3
    // statistic = 0.03717820461517941
    // pValue = 0.9981145601231336
    // #################################################################################################################

    // 键值对也可以进行独立性检验:
    val lines: RDD[String] = sc.textFile("src/main/data/iris.data")
    val labelRDD: RDD[LabeledPoint] = lines.map(x => {
      val parts: Array[String] = x.split(",")
      LabeledPoint(
        if (parts(4) == "Iris-setosa")
          0.toDouble
        else if (parts(4) == "Iris-versicolor")
          1.toDouble
        else
          2.toDouble
        , Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble)
      )
    })
    // 进行独立性检验，返回一个包含每个特征对于标签的卡方检验的数组
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(labelRDD)
    println(s"featureTestResults: *******************")
    featureTestResults.foreach(println)
    // #################################################################################################################

    // 生成一个随机的double RDD，其值是标准的正态分布N（0， 1），然后将其映射到N（1，4）
    // 生成1000000个服从N（0，1）正态分布的RDD[Double]，分别放在10个分区里
    //
    val u: RDD[Double] = RandomRDDs.normalRDD(sc, 1000000L, 10)
    val v: RDD[Double] = u.map(x => 1.0 * 2.0 * x)
    println(s"v: ${v.collect.toBuffer}")
    // #################################################################################################################

    // 核密度估计
    val test: RDD[Double] = sc.textFile("src/main/data/iris.data").map(_.split(",")).map(p => p(0).toDouble)
    // 用样本数据构建核函数
    val kd: KernelDensity = new KernelDensity().setSample(test).setBandwidth(3.0)
    // setBandwidth表示高斯核的宽度，为一个平滑参数，可以看作是高斯核的标准差
    // 构造了核密度估计kd，就可以对给定的数据进行核估计
    val densities: Array[Double] = kd.estimate(Array(-1.0, 2.0, 5.0, 5.8))
    println("densities: " + densities)


    spark.close()
  }

}
