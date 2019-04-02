package socket

import java.net.Socket

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:42 2019/4/2
  * @ 
  */
class Handler(socket: Socket) extends Runnable{
  override def run(): Unit = {
    val message: Array[Byte] = (Thread.currentThread().getName+"\n").getBytes()

    socket.getOutputStream.write(message)
    socket.getOutputStream.close()
  }
}
