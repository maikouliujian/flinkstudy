package stream.timer

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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


/***
  * 在flink实时处理中，涉及到延时处理可使用KeyedProcessFunction来完成，KeyedProcessFunction是flink提供面向用户的low level api，可以访问状态、当前的watermark或者当前的processingtime, 更重要的是提供了注册定时器的功能，分为：
  *
  * 注册处理时间定时器，直到系统的processingTime超过了注册的时间就会触发定时任务
  *
  * 注册事件时间定时器，直到watermark值超过了注册的时间就会触发定时任务
  * 另外也可以删除已经注册的定时器。
  *
  * 看一个实际案例：服务器下线监控报警，服务器上下线都会发送一条消息，如果发送的是下线消息，在之后的5min内没有收到上线消息则循环发出警告，直到上线取消告警。
  * 实现思路：
  * 1.由于根据服务器不在线时间来告警，应该使用ProcessingTime语义
  * 2.首先将服务器信息按照serverId分组，然后使用一个继承KeyedProcessFunction的类的Function接受处理，定义两个ValueState分别存储触发时间与服务器信息，
  *
  * open方法，初始化状态信息
  *
  * processElement方法，处理每条流入的数据，如果收到的是offline状态，则注册一个ProcessingTime的定时器，并且将服务器信息与定时时间存储状态中；如果收到的是online状态并且状态中定时时间不为-1，则删除定时器并将状态时间置为-1
  *
  * onTimer方法，定时回调的方法，触发报警并且注册下一个定时告警
  * 代码实现如下：
  *
  * https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483943&idx=1&sn=d3e1002255cbd6cfe16855639f82b80f&chksm=fe2b6668c95cef7e3639c575ac8cb899e7317f82c2b446a23335c8f33dc97a968ea7f345b914&scene=21#wechat_redirect
  *
  * @param serverId
  * @param isOnline
  * @param timestamp
  */

case class ServerMsg(serverId: String, isOnline: Boolean, timestamp: Long)


class MonitorKeyedProcessFunction extends KeyedProcessFunction[String, ServerMsg, String] {

  private var timeState: ValueState[Long] = _
  private var serverState: ValueState[String] = _


  override def open(parameters: Configuration): Unit = {
    timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", TypeInformation.of(classOf[Long])))
    serverState = getRuntimeContext.getState(new ValueStateDescriptor[String]("server-state", TypeInformation.of(classOf[String])))
  }


  override def processElement(value: ServerMsg, ctx: KeyedProcessFunction[String, ServerMsg, String]#Context, out: Collector[String]): Unit = {
    if (!value.isOnline) {
      val monitorTime = ctx.timerService().currentProcessingTime() + 300000
      timeState.update(monitorTime)
      serverState.update(value.serverId)
      ctx.timerService().registerProcessingTimeTimer(monitorTime)
    }

    //代表之前下过线，又上线了，删除注册的定时服务
    if (value.isOnline && -1 != timeState.value()) {
      ctx.timerService().deleteProcessingTimeTimer(timeState.value())
      timeState.update(-1)
    }

  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ServerMsg, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时服务的回调逻辑，如果触发，提出警告，然后再接着注册
    if (timestamp == timeState.value()) {
      val newMonitorTime = timestamp + 300000
      timeState.update(newMonitorTime)
      ctx.timerService().registerProcessingTimeTimer(newMonitorTime)
      println("告警:" + serverState.value() + " is offline, please restart")

    }

  }

}
