package rpc.akka.actors

import akka.actor.Actor

class BActor extends Actor{
  override def receive:Receive = {
    case "我打" => {
      println("BActor(乔峰) 挺猛 看我降龙十八掌")
      Thread.sleep(1000)
      //通过sender() 可以获取到发现消息的actor的ref
      sender() ! "我打"
    }
  }
}
