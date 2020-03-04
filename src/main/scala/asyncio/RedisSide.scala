package asyncio

import java.util.function.Consumer


import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import org.apache.commons.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

class RedisSide extends RichAsyncFunction[AdData, AdData] {

  private var redisClient: RedisClient = _
  private var connection: StatefulRedisConnection[String, String] = _
  private var async: RedisAsyncCommands[String, String] = _

  def open(parameters: Configuration): Unit = {
    val redisUri = "redis://localhost"
    redisClient = RedisClient.create(redisUri)
    connection = redisClient.connect()
    async = connection.async()
  }


  override def asyncInvoke(input: AdData, resultFuture: ResultFuture[AdData]): Unit = {
    import scala.collection.JavaConverters._
    val tid = input.tId.toString
    async.hgetall(tid).thenAccept(new Consumer[java.util.Map[String, String]]() {
      override def accept(t: java.util.Map[String, String]): Unit = {

        if (t == null || t.size() == 0) {

          resultFuture.complete(java.util.Arrays.asList(input).asScala)
          return
        }
        //scala map的底层是二元组实现
        t.asScala.foreach(x => {
          if ("aid".equals(x._1)) {
            val aid = x._2.toInt
            var newData = AdData(aid, input.tId, input.clientId, input.actionType, input.time)
            resultFuture.complete(java.util.Arrays.asList(newData).asScala)
          }
        })
      }
    })
  }
  //关闭资源
  override def close(): Unit = {
    if (connection != null) connection.close()
    if (redisClient != null) redisClient.shutdown()
  }

}
