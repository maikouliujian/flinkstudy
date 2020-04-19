package rpc.akka.actors

import akka.actor.{ActorRef, ActorSystem, Props}

//100招后，就退出
object ActorGame extends App {
  //创建ActorSystem
  val actorfactory = ActorSystem("actorfactory")
  //先创建BActor引用/代理
  val bActorRef: ActorRef = actorfactory.actorOf(Props[BActor], "bActor")
  //创建AActor的引用
  val aActorRef: ActorRef = actorfactory.actorOf(Props(new AActor(bActorRef)), "aActor")

  //aActor出招
  aActorRef ! "start"
}
