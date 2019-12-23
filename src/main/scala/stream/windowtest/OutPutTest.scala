package stream.windowtest

import akka.io.Tcp.Bound
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import stream.state.SensorReading


object OutPutTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val stream2 = env.readTextFile("D:\\idea\\project\\flinkstudy\\data\\sensor.txt")
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
          dataArray(2).trim.toDouble)
      })
      //.assignAscendingTimestamps(_.timestamp*1000)       //如果是升序数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    val stream3: DataStream[SensorReading] = stream2.process(new FreezingMonitor())



    stream2.print("mainout")
    //获取测输出流
    stream3.getSideOutput(new OutputTag[String]("freezing-alarms")).print("output")
    env.execute()
  }


  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
    // 定义一个侧输出标签
   // lazy val freezingAlarmOutput: OutputTag[SensorReading] = new OutputTag[SensorReading]("freezing-alarms")
    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 温度在 32F 以下时，输出警告信息
      if (r.temperature < 32.0) {
        ctx.output(new OutputTag[String]("freezing-alarms"), s"Freezing Alarm for ${r.id}")
      }

      // 所有数据直接常规输出到主流
      out.collect(r)

    }
  }



}
