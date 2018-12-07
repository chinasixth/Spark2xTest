package com.qinglianyun.jdbc

import scala.collection.mutable.ArrayBuffer

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:57 2018/12/5
  * @ 
  */
object Test {
  //  def main(args: Array[String]): Unit = {
  //    val s = Array[Any]("hello", "world", "I", "love", 2.2)
  //    val ss = ArrayBuffer("hello", "world", "I", "love")
  //    // 使用ArrayBuffer不能加泛型
  //    test(1, s:_*)
  //    println("-===========================")
  //    TT.test(1, ss:_*)
  //  }
  //
  //  def test(a: Int, args: Any*): Unit = {
  //    args.foreach(println)
  //  }

  def main(args: Array[String]): Unit = {
    //    val params = new ArrayBuffer[Array[Any]]()
    //
    //    params.+=(Array("name", 1))
    //    println("params: " + params.length)
    //    for (param <- params) {
    //      for (i <- 0 until param.length) {
    //        println(param(i))
    //      }
    //    }

//    val utils: JDBCUtils = JDBCUtils.getInstance()
//    val i: Int = utils.insert("insert into person values(?,?)", "name", 19)
//    println(i)

    val utils: JDBCUtils = JDBCUtils.getInstance()
    val sql = "delete from person where name = ? and age = ?"
    val params = ArrayBuffer("name", 19)
    utils.delete(sql, params)
    println("执行完了。。。")
  }

}
