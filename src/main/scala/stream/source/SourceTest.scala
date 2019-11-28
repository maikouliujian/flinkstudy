package stream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import stream.source.SourceTest.SensorReading

import scala.util.Random

object SourceTest {

  case class SensorReading(id:String,timestamp:Long,temperature:Double)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val streami: DataStream[SensorReading] = env.fromCollection(List(
//      SensorReading("sensor_1", 1547718199, 35.80018327300259),
//      SensorReading("sensor_6", 1547718201, 15.402984393403084),
//      SensorReading("sensor_7", 1547718202, 6.720945201171228),
//      SensorReading("sensor_10", 1547718205, 38.101067604893444)
//    ))
//    streami.print().setParallelism(1)
//    env.execute()

    //3、flink对接kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val datas: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))



    //4、自定义source

     val value: DataStream[SensorReading] = env.addSource(new SensorSource())
    value.print("kafka").setParallelism(1)
    env.execute("source test")


  }

}


class SensorSource extends SourceFunction[SensorReading] {

  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + random.nextGaussian() * 20)
    )
    while (running) {
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)

    }
  }
}
