package joins

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

case class Order1(orderId: String, userId: String, gdsId: String, amount: Double, addrId: String, time: Long)

case class Address(addrId: String, userId: String, address: String, time: Long)

//case class RsInfo(orderId: String, userId: String, gdsId: String, amount: Double, addrId: String, address: String)

object IntervalJoinDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(5000L)
    env.setParallelism(1)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")

    import org.apache.flink.api.scala._
    val orderConsumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema, kafkaConfig)
    val addressConsumer = new FlinkKafkaConsumer011[String]("topic2", new SimpleStringSchema, kafkaConfig)
    val orderStream = env.addSource(orderConsumer)
      .map(x => {
        val a = x.split(",")
        Order1(a(0), a(1), a(2), a(3).toDouble, a(4), a(5).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order1](Time.seconds(10)) {
      override def extractTimestamp(element: Order1): Long = element.time
    }).keyBy(_.addrId)

    val addressStream = env.addSource(addressConsumer)
      .map(x => {
        val a = x.split(",")
        new Address(a(0), a(1), a(2), a(3).toLong)

      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Address](Time.seconds(10)) {
      override def extractTimestamp(element: Address): Long = element.time

    }).keyBy(_.addrId)


    orderStream.intervalJoin(addressStream)
      .between(Time.seconds(1), Time.seconds(5))
      .process(new ProcessJoinFunction[Order1, Address, RsInfo] {
        override def processElement(left: Order1, right: Address, ctx: ProcessJoinFunction[Order1, Address, RsInfo]#Context, out: Collector[RsInfo]): Unit = {

          println("==在这里得到相同key的两条数据===")
          println("left:" + left)
          println("right:" + right)

        }

      })

    env.execute()

  }

}