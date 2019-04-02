import socket.NetworkService

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:33 2019/4/2
  * @ 
  */
object SingleThreadAndSocket {
  def main(args: Array[String]): Unit = {
    new NetworkService(2019, 2).run()
  }

  def syn(j: Int = 0): Unit = {
    var i = 3

    this.synchronized {
      i += j
      println(i)
    }
  }
}
