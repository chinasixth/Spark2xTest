package Akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:00 2019/4/2
  * @ 
  */
class PersonControl extends Actor with Serializable {

  override def preStart(): Unit = {
    super.preStart()
    println("控制中心正在启动......")
  }

  override def receive: Receive = {
    case "start" => println("控制中心已打开")
    case person: Person => person.toString
      sender ! "注册成功"
    case "connect" => println("有人连接......")
    case _ => println("你他妈发的啥")
  }
}

object PersonControl {
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = "7777"

    val configStr: String =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    val config: Config = ConfigFactory.parseString(configStr)

    val actorSystem: ActorSystem = ActorSystem.create("PersonControl", config)

    val actorRef: ActorRef = actorSystem.actorOf(Props[PersonControl], "Control")

    actorRef ! "start"
  }
}