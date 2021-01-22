package stream.tragger

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer011}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @author lj
  * @createDate 2020/3/2 17:48
  **/

case class AreaOrder(areaId: String, amount: Double)
case class Order(orderId: String, orderTime: Long, gdsId: String, amount: Double, areaId: String)

object ContinuousEventTimeTriggerDemo {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(5000L)
    env.setParallelism(1)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")

    val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), kafkaConfig)
    import org.apache.flink.api.scala._

    env.addSource(consumer)
      .map(data=>{
        val a = data.split(",")
        Order(a(0),a(1).toLong, a(2), a(3).toDouble, a(4))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(30)) {

      override def extractTimestamp(element: Order): Long = element.orderTime

    }).map(x => {
      AreaOrder(x.areaId, x.amount)
    })
      .keyBy(_.areaId)
      .timeWindow(Time.hours(1))
      .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1))) //TODO 一个小时的窗口，一分钟触发一个计算
      .reduce(new ReduceFunction[AreaOrder] {
        override def reduce(value1: AreaOrder, value2: AreaOrder): AreaOrder = {
          AreaOrder(value1.areaId, value1.amount + value2.amount)
        }
      }).print()

    env.execute()

  }

}
