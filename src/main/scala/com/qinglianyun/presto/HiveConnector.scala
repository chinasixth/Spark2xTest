package com.qinglianyun.presto

import java.sql.{DriverManager, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.util.TimeZone

import com.facebook.presto.jdbc.PrestoConnection
import org.slf4j.{Logger, LoggerFactory}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object HiveConnector {
  private val logger: Logger = LoggerFactory.getLogger("HiveConnector")

  def main(args: Array[String]): Unit = {
    // jdbc:presto://host:port/catalog/schema
    // 其中 catalog 和 schema 是可以省略的，可以实现跨库查询
    //    val hiveUrl = "jdbc:presto://139.217.83.148:8001/hive/default"
    val kafkaUrl = "jdbc:presto://139.217.83.148:8001/kafka/default"

    val conn: PrestoConnection = connect(kafkaUrl)
    //    val sql = "select * from hive.default.dtest"
    val sql = "select * from kafka.default.test"
    var ps: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      ps = conn.prepareStatement(sql)
      resultSet = ps.executeQuery()
      printKafkaResultSet(resultSet)
      conn.commit()
    } catch {
      case e: Exception =>
        conn.rollback()
        logger.error(e.getMessage)
    } finally {
      closeResultSet(resultSet)
      closePreparedStatement(ps)
      close(conn)
    }
  }

  /**
    * 打印 Kafka Connector 获取的数据集
    *
    * @param resultSet 数据集
    */
  def printKafkaResultSet(resultSet: ResultSet): Unit = {
    while (resultSet.next()) {
      val message: String = resultSet.getString("_message")
      println(s"message: $message")
    }
  }

  /**
    * 打印 Hive Connector 获取的数据集
    *
    * @param resultSet 数据集
    */
  def printHiveResultSet(resultSet: ResultSet): Unit = {
    val mac: String = resultSet.getString("mac")
    val phoneBrand: String = resultSet.getString("phone_brand")
    val enterName: Timestamp = resultSet.getTimestamp("enter_time")
    val firstName: Timestamp = resultSet.getTimestamp("first_time")
    val lastName: Timestamp = resultSet.getTimestamp("last_time")
    val region: String = resultSet.getString("region")
    val screen: String = resultSet.getString("screen")
    val stayLong: Int = resultSet.getInt("stay_long")

    print(
      s"""
         |mac: $mac
         |phone_brand: $phoneBrand
         |enter_time: $enterName
         |first_time: $firstName
         |last_time: $lastName
         |region: $region
         |screen: $screen
         |stay_long: $stayLong
           """.stripMargin)
  }

  def connect(url: String): PrestoConnection = {
    //  必须设置
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
    try {
      Class.forName("com.facebook.presto.jdbc.PrestoDriver")
    } catch {
      case e: ClassNotFoundException => logger.error(e.getMessage)
    }
    var connect: PrestoConnection = null
    try {
      connect = DriverManager.getConnection(url, "hive", null).asInstanceOf[PrestoConnection]
      connect.setAutoCommit(false)
    } catch {
      case e: SQLException => logger.error(e.getMessage)
    }
    connect
  }

  def close(conn: PrestoConnection): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }

  def closePreparedStatement(ps: PreparedStatement): Unit = {
    try {
      if (ps != null) {
        ps.close()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }

  def closeResultSet(rs: ResultSet): Unit = {
    try {
      if (rs != null) {
        rs.close()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }

}
