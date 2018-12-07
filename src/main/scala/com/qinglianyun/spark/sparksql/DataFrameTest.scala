package com.qinglianyun.spark.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:48 2018/11/28
  * @ desc   ：使用SparkSession读取json/csv格式的文件，并注册成表，使用SparkSession.sql进行分析
  *
  * 读取json文件
  * 注意：1.对于json格式的文件，一个json串必须在一行，否则报错
  *      2.读取json格式的文件需要指定schema，否则报错；指定schema的时候，名字应该和schema中的一致
  *      3.使用sql查询已经注册的表时，如果使用的是select *，那么结果将会按照列名的字典顺序排序
  *      4.操作相应的源，可以写上format()，比如format("json")。value：console、json、kafka、csv、orc、jdbc、text、libsvm、parquet
  *      5.如果写上了format，后面可以直接使用load方法读取数据；如果没有写，可以直接使用已经封装好的方法，比如.json
  *
  *
  * 读取csv格式的文件
  * 注意：1.默认分隔符是","，可以使用.option("sep", ";")指定分割符
  *      2.如果不指定option("header", "true")，csv文件中的第一行将不会被作为列名，而是由系统默认创建列名
  *
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    //    val schema = new StructType(
    //      Array(
    //        new StructField("name", DataTypes.StringType, true),
    //        new StructField("age", DataTypes.ShortType, true)
    //      )
    //    )

    // 针对复杂的数据源，可以采用这种方式读取
    //    val sourceDF = spark.read
    //      .format("csv")
    //      .option("","")
    //      .load("")

    // 一个json只能在一行，在多行就会报错，也可能是我的schema写的有问题。
    // 读取json格式的文件，不用指定schema，一个json串必须在同一行
    //
    //    // TODO 测试DataFrame
    //    // TODO 读取json文件
    //    val frame: DataFrame = spark.read
    //      // format("json")可指定可不指定
    //      .format("json")
    //      .json("src/main/data/people.json")
    //
    //    frame.show()
    //
    //    // 将DataFrame注册成一张表，可以使用sql语句进行查询
    //    frame.createOrReplaceTempView("people")
    //    // 使用sql语句查询
    //    val select = spark.sql("select * from people")
    //    // 展示结果
    //    select.show()

    //    // TODO 读取csv文件
    //    val csvDF: DataFrame = spark.read
    //      .format("csv")
    //      .option("sep", ",")
    //      //      .option("inferSchema", "true")
    //      .option("header", "true")
    //      .load("src/main/data/people.csv")
    //
    //    // 创建全局的视图，在查询时，必须使用“global_temp”引用视图名
    //    csvDF.createOrReplaceGlobalTempView("people")
    //    val csvSelect = spark.sql("select * from global_temp.people")
    //    csvSelect.show()

    //    // TODO 读取parquet文件
    //    val parquetDF: DataFrame = spark.read.parquet("src/main/data/users.parquet")
    //    parquetDF.show()
    //    // 保存，生成的时文件夹，要保证此文件夹不存在
    //    //    parquetDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    //    // 当读进来的格式和write的格式不一样时，可以手动指定输出的格式。
    //    parquetDF.select("name", "favorite_color").write.format("parquet").save("namesAndFavColors.parquet")

    val jsonDF: DataFrame = spark.read.format("json")
      .option("multiLine", "true")
      .load("src/main/data/people.json")
    jsonDF.show()


    spark.close()
  }
}
