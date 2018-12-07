package com.qinglianyun.jdbc

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.DbUtils

import scala.collection.mutable.ArrayBuffer


/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:48 2018/12/4
  * @ 
  */
class JDBCUtils private {
  var dataSource: ComboPooledDataSource = null

  def getConnectPool(): ComboPooledDataSource = {
    dataSource = new ComboPooledDataSource("qlcloud_c3p0")
    dataSource
  }

  /*
  * 插入一条数据，返回受影响的行数
  * */
  def insert(sql: String, params: Any*): Int = {
    val conn = dataSource.getConnection()
    val ps = conn.prepareStatement(sql)
    for (i <- 0 until params.length) {
      ps.setObject(i + 1, params(i))
    }
    val result = ps.execute()
    DbUtils.close(ps)
    DbUtils.close(conn)
    if (result) {
      0
    } else {
      1
    }
  }

  def insert(sql: String, params: ArrayBuffer[Any]): Int = {
    val conn = dataSource.getConnection()
    val ps = conn.prepareStatement(sql)
    for (i <- 0 until params.length) {
      ps.setObject(i + 1, params(i))
    }
    val bool: Boolean = ps.execute()
    DbUtils.close(ps)
    DbUtils.close(conn)
    if (bool) {
      0
    } else {
      1
    }
  }

  /*
  * 批量插入，返回受影响的行数
  * 要先关闭conn的自动提交
  * 如果关闭了自动提交，一定要手动提交，要不然数据库中是没有结果的。
  * 尽量按照分区和条数两个标准插入数据。
  * */
  def insertBatch(sql: String, params: ArrayBuffer[Array[Any]]): Int = {
    val conn = dataSource.getConnection()
    val ps = conn.prepareStatement(sql)
    conn.setAutoCommit(false)

    for (param <- params) {
      for (i <- 0 until param.length) {
        ps.setObject(i + 1, param(i).asInstanceOf[Object])
      }
      ps.addBatch()
    }
    val ints: Array[Int] = ps.executeBatch()
    conn.commit()
    DbUtils.close(ps)
    DbUtils.close(conn)
    ints.sum
  }

  /*
  * 删除数据
  * */
  def delete(sql: String, params: ArrayBuffer[Any]): Unit = {
    val conn = dataSource.getConnection()
    val ps = conn.prepareStatement(sql)
    for (i <- 0 until params.length) {
      ps.setObject(i + 1, params(i))
    }
    ps.execute()
    DbUtils.close(ps)
    DbUtils.close(conn)
  }

  /*
  * 批量删除数据
  * */
  def deleteBatch(sql: String, params: ArrayBuffer[Array[Any]]): Unit = {
    val conn = dataSource.getConnection()
    val ps = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    for (param <- params) {
      for (i <- 0 until param.length) {
        ps.setObject(i + 1, param(i).asInstanceOf[Object])
      }
      ps.addBatch()
    }
    ps.executeBatch()
    conn.commit()
    DbUtils.close(ps)
    DbUtils.close(conn)
  }

}


/*
* scala中的单例模式：
* 根据scala的特性，使用class的伴生对象，实现单例模式，
* 因为class的伴生对象是唯一的，
* 所以通过伴生对象来调用class的构造函数，且在构造函数中实例化一个对象，那么这个对象也就是唯一的了。
* */
object JDBCUtils {
  private val jdbcUtils = new JDBCUtils

  def getInstance(): JDBCUtils = {
    jdbcUtils.getConnectPool()
    jdbcUtils
  }
}
