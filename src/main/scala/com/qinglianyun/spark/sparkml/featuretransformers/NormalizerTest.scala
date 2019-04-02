package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:07 2019/1/15
  * @ desc: Normalizer是一个转换器，它转换向量行数据集（即输入的是向量列），作用于每一行
  * 将每个向量（注意是向量）归一化成具有单位范数的向量。它接收参数p，用来指定范数，默认（p=2）
  * 这种归一化可以帮助标准换输入数据和改进学习算法。
  *
  * 归一化：
  * 1）就是把数据变成（0， 1）和（-1， 1）之间的小数，主要是为了数据处理方便提出来的。
  * 2）把有量纲表达式转换成无量纲表达式，便于不同单位或量级的指标能够进行比较和加权。
  *
  *
  * 什么学习算法需要归一化，为什么归一化，详情参考下方链接
  * 参考资料：https://www.jianshu.com/p/95a8f035c86c
  *
  * L1范数和L2范数的区别，参考下方资料：
  * https://blog.csdn.net/u014381600/article/details/54341317/
  *
  * L0范数是指向量中非0的元素的个数。如果我们用L0范数来规则化一个参数矩阵W的话，就是希望W的大部分元素都是0。
  * 这太直观了，太露骨了吧，换句话说，让参数W是稀疏的。OK，看到了“稀疏”二字，大家都应该从当下风风火火的“压缩感知”和“稀疏编码”中醒悟过来，
  * 原来用的漫山遍野的“稀疏”就是通过这玩意来实现的。但你又开始怀疑了，是这样吗？看到的papers世界中，稀疏不是都通过L1范数来实现吗？
  * 脑海里是不是到处都是||W||1影子呀！几乎是抬头不见低头见。没错，这就是这节的题目把L0和L1放在一起的原因，因为他们有着某种不寻常的关系。
  * 那我们再来看看L1范数是什么？它为什么可以实现稀疏？为什么大家都用L1范数去实现稀疏，而不是L0范数呢？
  *
  * L1范数是指向量中各个元素绝对值之和，也有个美称叫“稀疏规则算子”（Lasso regularization）。
  * 现在我们来分析下这个价值一个亿的问题：为什么L1范数会使权值稀疏？有人可能会这样给你回答“它是L0范数的最优凸近似”。
  * 实际上，还存在一个更美的回答：任何的规则化算子，如果他在Wi=0的地方不可微，并且可以分解为一个“求和”的形式，那么这个规则化算子就可以实现稀疏。
  * 这说是这么说，W的L1范数是绝对值，|w|在w=0处是不可微，但这还是不够直观。这里因为我们需要和L2范数进行对比分析。所以关于L1范数的直观理解，请待会看看第二节。
  *
  * 对了，上面还有一个问题：既然L0可以实现稀疏，为什么不用L0，而要用L1呢？
  * 个人理解一是因为L0范数很难优化求解（NP难问题），二是L1范数是L0范数的最优凸近似，而且它比L0范数要容易优化求解。
  * 所以大家才把目光和万千宠爱转于L1范数。
  *
  * OK，来个一句话总结：L1范数和L0范数可以实现稀疏，L1因具有比L0更好的优化求解特性而被广泛应用。
  *
  * 好，到这里，我们大概知道了L1可以实现稀疏，但我们会想呀，为什么要稀疏？让我们的参数稀疏有什么好处呢？这里扯两点：
  * 第一，
  * 大家对稀疏规则化趋之若鹜的一个关键原因在于它能实现特征的自动选择。
  * 一般来说，xi的大部分元素（也就是特征）都是和最终的输出yi没有关系或者不提供任何信息的，
  * 在最小化目标函数的时候考虑xi这些额外的特征，虽然可以获得更小的训练误差，但在预测新的样本时，这些没用的信息反而会被考虑，从而干扰了对正确yi的预测。
  * 稀疏规则化算子的引入就是为了完成特征自动选择的光荣使命，它会学习地去掉这些没有信息的特征，也就是把这些特征对应的权重置为0。
  * 第二，
  * 另一个青睐于稀疏的理由是，模型更容易解释。
  * 例如患某种病的概率是y，然后我们收集到的数据x是1000维的，也就是我们需要寻找这1000种因素到底是怎么影响患上这种病的概率的。
  * 假设我们这个是个回归模型：y=w1*x1+w2*x2+…+w1000*x1000+b（当然了，为了让y限定在[0,1]的范围，一般还得加个Logistic函数）。
  * 通过学习，如果最后学习到的w*就只有很少的非零元素，例如只有5个非零的wi，那么我们就有理由相信，这些对应的特征在患病分析上面提供的信息是巨大的，决策性的。
  * 也就是说，患不患这种病只和这5个因素有关，那医生就好分析多了。但如果1000个wi都非0，医生面对这1000种因素，累觉不爱。
  *
  *
  * Normalizer、StandardScaler、MinMaxScaler、MaxAbsScaler的区别：
  * https://www.cnblogs.com/nucdy/p/7994542.html
  *
  */
object NormalizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(
      Seq(
        (0, Vectors.dense(1.0, 0.5, -1.0)),
        (1, Vectors.dense(2.0, 1.0, 1.0)),
        (2, Vectors.dense(4.0, 10.0, 2.0))
      )
    ).toDF("id", "features")
    // +---+--------------+
    // |id |features      |
    // +---+--------------+
    // |0  |[1.0,0.5,-1.0]|
    // |1  |[2.0,1.0,1.0] |
    // |2  |[4.0,10.0,2.0]|
    // +---+--------------+

    // 使用1范式正则化
    val normalizer: Normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("norFeatures")
      .setP(3) // default: p = 2; 必须大于等于1

    val normalizered: DataFrame = normalizer.transform(dataset)

    normalizered.show(false)
    // 默认的范数
    // +---+--------------+-----------------------------------------------------------+
    // |id |features      |norFeatures                                                |
    // +---+--------------+-----------------------------------------------------------+
    // |0  |[1.0,0.5,-1.0]|[0.6666666666666666,0.3333333333333333,-0.6666666666666666]|
    // |1  |[2.0,1.0,1.0] |[0.8164965809277261,0.4082482904638631,0.4082482904638631] |
    // |2  |[4.0,10.0,2.0]|[0.3651483716701107,0.9128709291752769,0.18257418583505536]|
    // +---+--------------+-----------------------------------------------------------+
    // 设置范数为1
    // +---+--------------+------------------+
    // |id |features      |norFeatures       |
    // +---+--------------+------------------+
    // |0  |[1.0,0.5,-1.0]|[0.4,0.2,-0.4]    |
    // |1  |[2.0,1.0,1.0] |[0.5,0.25,0.25]   |
    // |2  |[4.0,10.0,2.0]|[0.25,0.625,0.125]|
    // +---+--------------+------------------+

    spark.close()
  }

}