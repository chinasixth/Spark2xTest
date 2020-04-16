import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object TestGroup {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger(TestGroup.getClass).setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestGroup")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    //        val data: DataFrame = spark.createDataFrame(Seq(
    //          (1, "nana", 18),
    //          (1, "nana", 19),
    //          (1, "nana", 9),
    //          (2, "xiaohong", 18),
    //          (2, "xiaohong", 18),
    //          (2, "xiaohong", 18),
    //          (2, "xiaohong", 18)
    //        )).toDF("id", "name", "age")
    //
    //        data.orderBy("id", "name").show()
    //        data.select(data.col("id"), data.col("name"),
    //          functions.sum("age") over Window.partitionBy("id", "name").orderBy("age").rowsBetween(-1, 0))
    //          .show()
    //    val A: Seq[(String, String)] = Seq[(String, String)](
    //      ("2019-01-01 00:00:00", "2019-01-31 23:59:59"),
    //      ("2019-02-01 00:00:00", "2019-02-28 23:59:59"),
    //      ("2019-03-01 00:00:00", "2019-03-31 23:59:59"),
    //      ("2019-04-01 00:00:00", "2019-04-30 23:59:59"),
    //      ("2019-05-01 00:00:00", "2019-05-31 23:59:59")
    //    )
    //
    //    val B: Seq[(String, Int)] = Seq[(String, Int)](
    //      ("2019-01-05 12:12:12", 1),
    //      ("2019-01-06 17:12:12", 1),
    //      ("2019-01-07 12:12:12", 1),
    //      ("2019-01-08 12:12:12", 1),
    //      ("2019-01-09 12:12:12", 1),
    //      ("2019-03-05 12:32:12", 1),
    //      ("2019-04-05 12:12:12", 1),
    //      ("2019-03-05 12:19:12", 1),
    //      ("2019-06-05 12:12:12", 1),
    //      ("2019-07-05 11:12:12", 1),
    //      ("2019-03-05 12:12:12", 1),
    //      ("2019-02-05 12:52:12", 1),
    //      ("2019-05-05 12:12:12", 1),
    //      ("2019-06-05 10:12:12", 1)
    //    )
    //
    //    val a: DataFrame = spark.createDataFrame(A).toDF("st", "et")
    //    val b: DataFrame = spark.createDataFrame(B).toDF("t", "count")
    //
    //    a.join(b)
    //        .where($"st" < $"t" and $"et" > $"t")
    //      .orderBy($"st")
    //        .show(false)

    //    val lateral = Seq(
    //      Lateral("front_page", Array(1, 2, 3)),
    //      Lateral("contact_page", Array(3, 4, 5))
    //    )
    //    spark.createDataFrame(lateral).toDF().createOrReplaceTempView("lat")
    //    spark.sql("select pageId, idList, id from lat lateral view explode(idList) temp_view as id").show(200)
    //    spark.sql("select pageId, idList, explode(idList) id from lat").show(false)
    //    spark.sql("select pageId, idList, explode_outer(array()) from lat").show(false)
    //    spark.sql("select pageId, idList, posexplode(idList) from lat").show(false)
    //    spark.sql("SELECT posexplode(map('a',11, 'b',22, 'c',33)) as ").show(false)

    //    val orders = Seq(
    //      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("深圳", "钻石会员", "钻石会员3个月", 70),
    //      MemberOrderInfo("深圳", "钻石会员", "钻石会员12个月", 300),
    //      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
    //      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
    //      MemberOrderInfo("深圳", "铂金会员", "铂金会员6个月", 120),
    //      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
    //      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
    //      MemberOrderInfo("深圳", "黄金会员", "黄金会员3个月", 45),
    //      MemberOrderInfo("深圳", "黄金会员", "黄金会员12个月", 180),
    //      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("北京", "铂金会员", "铂金会员3个月", 60),
    //      MemberOrderInfo("北京", "黄金会员", "黄金会员3个月", 45),
    //      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
    //      MemberOrderInfo("上海", "铂金会员", "铂金会员3个月", 60),
    //      MemberOrderInfo("上海", "黄金会员", "黄金会员3个月", 45)
    //    )

    //把seq转换成DataFrame
    //    val memberDF: DataFrame = orders.toDF()
    //把DataFrame注册成临时表
    //    memberDF.createOrReplaceTempView("orderTempTable")

    //    spark.sql("select area, memberType, product, sum(price) from orderTempTable group by area, memberType, product").show(false)
    //    spark.sql("select area, memberType, product, sum(price) from orderTempTable group by area, memberType, product grouping sets(area, memberType, product)").show(100)
    //    park.sql("select area, memberType, product, sum(price) from orderTempTable group by area, memberType, product with rollup").show(100)
    //   spark.sql("select area, memberType, product, sum(price) from orderTempTable group by area, memberType, product with cube").show(100)
    //    spark.sql("select grouping(area) area_g, grouping(memberType) memberType_g, grouping(product) product_g, area, memberType, product, sum(price) from orderTempTable group by area, memberType, product with rollup").show(200)
    //    spark.sql("select grouping_id() area_id, grouping(area) area_g, grouping(memberType) memberType_g, grouping(product) product_g, area, memberType, product from orderTempTable group by area, memberType, product with rollup").show(200)
    //    spark.sql("select kurtosis(price) from orderTempTable").show(100)
    //    spark.sql("select *, explode(split(trim(area), '')) from orderTempTable limit 10").show(false)
    //    spark.sql("select area, memberType, product, price, explode_outer(array()) from orderTempTable").show(100)

    //    val te = Seq(
    //      ("a:shandong,b:beijing,c:hebei",
    //        "1,2,3,4,5,6,7,8,9",
    //        "[{\"source\":\"7fresh\",\"monthSales\":4900,\"userCount\":1900,\"score\":\"9.9\"},{\"source\":\"jd\",\"monthSales\":2090,\"userCount\":78981,\"score\":\"9.8\"},{\"source\":\"jdmart\",\"monthSales\":6987,\"userCount\":1600,\"score\":\"9.0\"}]")
    //    )
    //    spark.createDataFrame(te).toDF("area", "goodsId", "saleInfo")
    //      .createOrReplaceTempView("lateral_view")
    //    spark.sql("select explode(area), area, goodsId from lateral_view").show(false)
    //    spark.sql("select explode(goodsId),* from lateral_view").show(false)
    //    spark.sql("select area, goodsId, saleInfo, area_exp, goods_exp from lateral_view " +
    //      "lateral view explode(split(area, ','))a as area_exp " +
    //      "lateral view explode(split(goodsId, ','))b as goods_exp").show(false)
    //    spark.sql("select area, goodsId, explode(split(area, ',')) from lateral_view").show(false)
    //    spark.sql("select area, goodsId, area_exp, goods_exp from lateral_view lateral view explode(split(area, ',')) a as area_exp lateral view explode(split(goodsId, ',')) b as goods_exp").show(100)
    //    spark.sql("select * from lateral_view join orderTempTable on true").show(1000)
    //    spark.sql("SELECT area, goodsId, get_json_object( concat('{', j, '}'), '$.source') AS SOURCE, get_json_object( concat('{', j, '}'), '$.monthSales') AS monthSales, get_json_object( concat('{', j, '}'), '$.userCount') AS userCount, get_json_object( concat('{', j, '}'), '$.score') AS score FROM (SELECT area, goodsId, explode(split(regexp_replace(regexp_replace(saleInfo, '\\\\[\\\\{', ''), '}]', ''), '\\\\],\\\\{')) j FROM lateral_view)").show(false)

    val click = Seq(
      Click("{\"curpage\":\"android_search.html\",\"client_ip\":\"114.102.*.169\",\"terminal\":{\"mode\":\"SM-N9009\",\"manufacture\":\"samsung\",\"macAddress\":\"\",\"imei\":\"\",\"imsi\":\"\",\"uid\":\"e6dc1412aab8****\"},\"timestamp\":1552320467}"),
      Click("{\"curpage\":\"android_suggestion.html\",\"client_ip\":\"223.104.*.73\",\"terminal\":{\"mode\":\"F1-F2-F3-Y800-Y900-B5\",\"manufacture\":\"alps\",\"macAddress\":\"\",\"imei\":\"\",\"imsi\":\"\",\"uid\":\"d8bd2a0cc0a****\"},\"timestamp\":1552320494}"),
      Click("{\"curpage\":\"android_search.html\",\"client_ip\":\"113.194.*.31\",\"terminal\":{\"mode\":\"OPPO A83\",\"manufacture\":\"OPPO\",\"macAddress\":\"\",\"imei\":\"\",\"imsi\":\"\",\"uid\":\"6a9bdc0f3b*****\"},\"timestamp\":1552320655}"),
      Click("{\"curpage\":\"android_search.html\",\"client_ip\":\"111.60.*.71\",\"terminal\":{\"mode\":\"U25GT-C4YT\",\"manufacture\":\"CUBE\",\"macAddress\":\"\",\"imei\":\"\",\"imsi\":\"\",\"uid\":\"59722af35aba****\"},\"timestamp\":1552320678}"),
      Click("{\"curpage\":\"android_search.html\",\"client_ip\":\"123.171.*.89\",\"terminal\":{\"mode\":\"U22\",\"manufacture\":\"Allwinner\",\"macAddress\":\"\",\"imei\":\"\",\"imsi\":\"\",\"uid\":\"7d0607144c7****\"},\"timestamp\":1552320921}")
    )
    spark.createDataFrame(click).toDF().createOrReplaceTempView("click")
    //    spark.sql("select get_json_object(log, '$.curpage') curpage from click").show(100)
    //    spark.sql("select get_json_object(log, '$.terminal') terminals from click").show(100, truncate = false)
    //    spark.sql("select json_tuple(log, 'curpage', 'client_ip') as (curpage, client_ip) from click").show(false)
    //    spark.sql("select from_json(log, 'curpage string, client_ip string, timestamp timestamp, terminal string') from click").show(false)
    //    spark.sql("select to_json('1,2,3,4')")
    //    spark.sql("select from_json(log, 'curpage string, client_ip string, terminal map<string,string>') from click").show(false)
spark.sql("select from_json(log, 'curpage string, client_ip string, $.terminal.mode string') from click").show(false)

    spark.stop()
  }

  case class Click(log: String)

  case class MemberOrderInfo(area: String, memberType: String, product: String, price: Int)

  case class LaterView(area: Map[String, String], goodsId: Array[Int], saleInfo: String)

  case class Lateral(pageId: String, idList: Array[Int])

}
