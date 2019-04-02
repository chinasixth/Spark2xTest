import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors}

import scala.collection.immutable
import scala.math._
import scala.beans.BeanProperty

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:19 2019/4/2
  * @ 
  */
object VarAndOpeTest {
  @BeanProperty var k = 0

  def main(args: Array[String]): Unit = {
    println(1.to(10))

    println(min(1, Pi))

    for (i <- 1 to 10) {
      print(i + " ")
    }
    println()

    for (i <- 1 until 10) {
      print(i + " ")
    }
    var i = 0

    import scala.util.control.Breaks._

    // break 效果
    breakable {
      while (true) {
        if (i == 10) {
          break()
        }
        println(i)
        i += 1

      }
    }

    // continue效果
    for (j <- 1 to 10) {
      breakable {
        if (j == 5) {
          break()
        }
        println(j)
      }
    }

    println(formatDate(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2018-10-10 10:22:33")))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // 创建一个线程池,线程会一直存在。newCacheThreadPool则会有一个存活的时间
    // newSingleThreadExecutor 创建一个单线程执行器，该执行器可以安排命令在给定的延迟之后运行，或者定期执行。
    // newScheduledThreadExecutor 创建一个线程池，该线程池可以调度在给定延迟之后运行的命令，或者定期执行命令
    val service: ExecutorService = Executors.newFixedThreadPool(10)

    try {
      for (i <- 1 to 100) {
        service.execute(new Runnable {
          override def run(): Unit = {
            println(i + "   " + formatDate())
            Thread.sleep(1000)
          }
        })
      }
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      service.shutdown()
    }

    // yield 关键字的使用
    val result: immutable.IndexedSeq[Int] = for(i <- 1 to 10) yield i *2
    result.foreach(println)

  }

  def formatDate(datetime: java.util.Date = new Date()): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    dateFormat.format(datetime)
  }


}
