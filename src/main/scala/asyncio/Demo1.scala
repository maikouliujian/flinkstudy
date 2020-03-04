package asyncio

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig

//https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483901&idx=1&sn=46d1968c2b04aa795d4030146d006b8c&chksm=
// fe2b65b2c95ceca4ba572c554da7e464067ce4d366ab7e51575b36c3918d6d1a544e0db8edef&token=1414359448&lang=zh_CN&scene=21#wechat_redirect
case class AdData(aId: Int, tId: Int, clientId: String, actionType: Int, time: Long)
object Demo1 {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

    val consumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), kafkaConfig)

    env.addSource(consumer)
      .map(x => {
        val a: Array[String] = x.split(",")
        AdData(0, a(0).toInt, a(1), a(2).toInt, a(3).toLong) //默认aid为0
      })
      .flatMap(new SideFlatMapFunction)
      .print()

    env.execute()

  }

}
