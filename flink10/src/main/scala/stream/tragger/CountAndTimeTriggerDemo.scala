package stream.tragger

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author lj
  * @createDate 2019/12/26 11:07
  **/

import org.apache.flink.api.scala._

object TreiggerDemo {
  private val logger: Logger = LoggerFactory.getLogger("TreiggerDemo")
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val dataStream: DataStream[(Int, Int)] = env.fromCollection(List((1, 2), (3, 4), (1, 3)))
    //消费者配置
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "192.168.1.163:9092,192.168.1.164:9092,192.168.1.165:9092")
    consumerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    consumerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val kafkaSource = new FlinkKafkaConsumer[String]("bigdata", new SimpleStringSchema(),consumerProperties)

    //val sink = new FlinkKafkaProducer[String](topic + "_out", new SimpleStringSchema(), consumerProperties)

    val stream = env.addSource(kafkaSource)
      .map(s => {
        s
      })
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .trigger(CountAndTimeTrigger.of(10, Time.seconds(10)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          var count = 0

          elements.iterator.foreach(s => {
            count += 1
          })
          logger.info("this trigger have : {} item", count)
        }
      })
    }


}
