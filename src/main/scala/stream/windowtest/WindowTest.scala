package stream.windowtest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.util.Collector
import stream.state.SensorReading

object WindowTest {

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
      .assignTimestampsAndWatermarks(new MyTimeAssigner())
      .map(data => (data.id, data.temperature))
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.seconds(-8))) //TODO offset -8 代表东8时区
      // .timeWindow(Time.seconds(10)) //todo 底层也是调用 .window()

      //x代表同一分区的上一条数据，y代表当前这条数据
      .reduce((x, y) => (x._1, x._2.min(y._2)))


    stream2.print()
    env.execute()
  }

  class MyTimeAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bounded = 6000
    var maxTW = Long.MinValue

    override def getCurrentWatermark: Watermark = new Watermark(maxTW - bounded)

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      maxTW = maxTW.max(t.timestamp * 1000)
      t.timestamp * 1000
    }
  }

  /** *
    * # TODO 需求：监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升， 则报警。
    */
  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    // 保存上一个传感器温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.
      getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
    // 保存注册的定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.
      getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

    override def processElement(r: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      // 取出上一次的温度val prevTemp = lastTemp.value()
      // 取出上一次的温度
      val prevTemp = lastTemp.value()
      // 将当前温度更新到上一次的温度这个变量中
      lastTemp.update(r.temperature)
      val curTimerTimestamp = currentTimer.value()
      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        // 温度下降或者是第一个温度值，删除定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        // 清空状态变量
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
        // 温度上升且我们并没有设置定时器
        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }
    }

    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器 id 为: " + ctx.getCurrentKey + "的传感器温度值已经连续 1s 上升了。")
      currentTimer.clear()

    }
  }

}
