package rpc.akka.sparkmasterworker.master

import akka.actor.{Actor, ActorSystem, Props}

import com.typesafe.config.ConfigFactory
import rpc.akka.sparkmasterworker.common._

import scala.collection.mutable
import scala.concurrent.duration._

class SparkMaster extends Actor {
  //定义个hm,管理workers
  val workers = mutable.Map[String, WorkerInfo]()

  override def receive: Receive = {

    case "start" => {
      println("master服务器启动了...")
      //这里开始。。
      self ! StartTimeOutWorker
    }
    case RegisterWorkerInfo(id, cpu, ram) => {
      //接收到worker注册信息
      if (!workers.contains(id)) {
        //创建WorkerInfo 对象
        val workerInfo = new WorkerInfo(id, cpu, ram)
        //加入到workers
        workers += ((id, workerInfo))
        println("服务器的workers=" + workers)
        //回复一个消息，说注册成功
        sender() ! RegisteredWorkerInfo
      }
    }
    case HeartBeat(id) => {
      //更新对应的worker的心跳时间
      //1.从workers取出WorkerInfo
      val workerInfo = workers(id)
      workerInfo.lastHeartBeat = System.currentTimeMillis()
      println("master更新了 " + id + " 心跳时间...")
    }
    case StartTimeOutWorker => {
      println("开始了定时检测worker心跳的任务")
      import context.dispatcher
      //说明
      //1. 0 millis 不延时，立即执行定时器
      //2. 9000 millis 表示每隔3秒执行一次
      //3. self:表示发给自己
      //4. RemoveTimeOutWorker 发送的内容
      context.system.scheduler.schedule(0 millis, 9000 millis, self, RemoveTimeOutWorker)
    }
    //对RemoveTimeOutWorker消息处理
    //这里需求检测哪些worker心跳超时（now - lastHeartBeat > 6000），并从map中删除
    case RemoveTimeOutWorker => {
      //首先将所有的 workers 的 所有WorkerInfo
      val workerInfos = workers.values
      val nowTime = System.currentTimeMillis()
      //先把超时的所有workerInfo,删除即可
      workerInfos.filter(workerInfo => (nowTime - workerInfo.lastHeartBeat) > 6000)
        .foreach(workerInfo => workers.remove(workerInfo.id))
      println("当前有 " + workers.size + " 个worker存活的")
    }
  }
}

object SparkMaster {
  def main(args: Array[String]): Unit = {

    //这里我们分析出有3个host,port,sparkMasterActor
    if (args.length != 3) {
      println("请输入参数 host port sparkMasterActor名字")
      sys.exit()
    }

    val host = args(0)
    val port = args(1)
    val name = args(2)

    //先创建ActorSystem
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=${host}
         |akka.remote.netty.tcp.port=${port}
            """.stripMargin)
    val sparkMasterSystem = ActorSystem("SparkMaster", config)
    //创建SparkMaster -actor
    val sparkMasterRef = sparkMasterSystem.actorOf(Props[SparkMaster], s"${name}")
    //启动SparkMaster
    sparkMasterRef ! "start"
  }
}
