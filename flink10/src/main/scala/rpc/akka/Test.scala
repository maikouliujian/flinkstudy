package rpc.akka

import com.typesafe.config.ConfigFactory

object Test {
  def main(args: Array[String]): Unit = {
    val (clientHost, clientPort) = ("127.0.0.1", 9990)
    println(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$clientHost
         |akka.remote.netty.tcp.port=$clientPort
        """.stripMargin)
  }
}
