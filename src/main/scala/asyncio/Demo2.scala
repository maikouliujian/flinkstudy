package asyncio

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig

//https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247484085&idx=1&sn=
// 3f5a5754845e56c600c6f9a7de4e9bbb&chksm=fe2b66fac95cefec097b4c3b1f70f97482d7922bc5f0960e8df27ee008c443558ff67ecbab14&scene=21#wechat_redirect
//case class AdData(aId: Int, tId: Int, clientId: String, actionType: Int, time: Long)
object Demo2 {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")
    import org.apache.flink.api.scala._
    val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), kafkaConfig);
    val ds = env.addSource(consumer)
      .map(x => {
        val a: Array[String] = x.split(",")
        AdData(0, a(0).toInt, a(1), a(2).toInt, a(3).toLong) //默认给0
      })

    val redisSide: AsyncFunction[AdData, AdData] = new RedisSide
    AsyncDataStream.unorderedWait(ds, redisSide, 5L, TimeUnit.SECONDS, 1000)
      .print()
    env.execute("Demo1")

    env.execute()

  }

}
