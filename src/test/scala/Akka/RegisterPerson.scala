package Akka

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 15:52 2019/4/2
  * @ 
  */
class RegisterPerson extends Actor with Serializable {
  private val control: ActorSelection = context.actorSelection("akka.tcp://PersonControl@127.0.0.1:7777/user/Master")

  control ! "connect"

  control ! Person("langer", 18)

  override def preStart(): Unit = {
    super.preStart()
    println("可以注册方法被执行......")
  }

  override def receive: Receive = {
    case "start" => println("person start.....")
    case s: String => println(s)
    case _ => println("乱七八糟。。。。")
  }
}

object RegisterPerson {
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = "6666"

    val configStr: String =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    val config: Config = ConfigFactory.parseString(configStr)

    val actorSystem: ActorSystem = ActorSystem.create("RegisterPerson", config)

    val actorRef: ActorRef = actorSystem.actorOf(Props[RegisterPerson], "person")

    actorRef ! "start"
  }
}
