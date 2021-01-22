package study

import java.io.File
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * @author lj
  * @createDate 2019/12/27 11:17
  **/

case class Event(id: String, createTime: String, count: Int = 1)
import org.apache.flink.api.scala._

object KafkaConsumer {

  def main(args: Array[String]): Unit = {
    // println(1558886400000L - (1558886400000L - 8 + 86400000) % 86400000)
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    if ("\\".equals(File.pathSeparator)) {
      val rock = new RocksDBStateBackend("自己设置路径===")
      env.setStateBackend(rock)
      // checkpoint interval
      env.enableCheckpointing(10000)
    }

    val topic = "bigdata"
    //生产者配置
    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "192.168.1.163:2181,192.168.1.164:2181,192.168.1.165:2181")
    producerProperties.setProperty("group.id", "test-consumer-group")
    producerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    producerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    producerProperties.setProperty("auto.offset.reset", "latest")

    //消费者配置
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "192.168.1.163:9092,192.168.1.164:9092,192.168.1.165:9092")
    consumerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    consumerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    /*val kafkaSource = new FlinkKafkaConsumer[ObjectNode](topic, new JsonNodeDeserializationSchema(),consumerProperties)

    val sink = new FlinkKafkaProducer[String](topic + "_out", new SimpleStringSchema(), producerProperties)
    sink.setWriteTimestampToKafka(true)

    val stream = env.addSource(kafkaSource)
      .map(node => {
        Event(node.get("id").asText(), node.get("createTime").asText())
      })
      //            .assignAscendingTimestamps(event => sdf.parse(event.createTime).getTime)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(60)) {
      override def extractTimestamp(element: Event): Long = {
        sdf.parse(element.createTime).getTime
      }
    })
      // window is one minute, start at 0 second
      //.windowAll(TumblingEventTimeWindows.of(Time.minutes(1), Time.seconds(0)))
      // window is one hour, start at 0 second 注意事件时间，需要事件触发，在窗口结束的时候可能没有数据，有数据的时候，已经是下一个窗口了
      //      .windowAll(TumblingEventTimeWindows.of(Time.hours(1), Time.seconds(0)))
      // window is one day, start at 0 second, todo there have a bug(FLINK-11326), can't use negative number, 1.8 修复
      //      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))  //最后一个是东八区
      // every event one minute
      //      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(3800)))
      // every process one minute
      //      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
      // every event, export current value,
      //      .trigger(CountTrigger.of(1))
      .reduce(new ReduceFunction[Event] {
      override def reduce(event1: Event, event2: Event): Event = {

        // 将结果中，id的最小值和最大值输出
        new Event(event1.id, event2.id, event1.count + event2.count)
      }
    })
    // format output even, connect min max id, add current timestamp
    //      .map(event => Event(event.id + "-" + event.createTime, sdf.format(System.currentTimeMillis()), event.count))
    stream.print("result : ")
    // execute job
    env.execute("CurrentDayCount")*/
  }
}
