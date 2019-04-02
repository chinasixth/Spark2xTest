package socket

import java.net.{ServerSocket, Socket}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:38 2019/4/2
  * @ 
  */
class NetworkService(port: Int, pollSize: Int) extends Runnable{
  private val serverSocket: ServerSocket = new ServerSocket(port)

  override def run(): Unit = {
    while (true){
      val socket: Socket = serverSocket.accept()
      new Handler(socket).run()
    }
  }

}
