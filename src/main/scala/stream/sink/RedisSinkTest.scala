package stream.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import stream.source.SourceTest.SensorReading

object RedisSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream2 = env.readTextFile("D:\\idea\\project\\flinkstudy\\data\\sensor.txt")
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
          dataArray(2).trim.toDouble)
      })

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().
      setHost("localhost").
      setDatabase(0).build()


    stream2.addSink(new RedisSink(config,new MyRedisMapper))

  }

}


class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }
  override def getValueFromData(t: SensorReading): String =
    t.temperature.toString
  override def getKeyFromData(t: SensorReading): String = t.id
}
