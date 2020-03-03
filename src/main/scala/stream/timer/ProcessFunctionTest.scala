package stream.timer

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/24 10:14
  */

case class SensorReading(id:String,timestamp:Long,temperature:Double)


object ProcessFunctionTest {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(300), org.apache.flink.api.common.time.Time.seconds(10)))

//    env.setStateBackend( new RocksDBStateBackend("") )

    val stream = env.socketTextStream("localhost", 7777)


    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    val processedStream = dataStream.keyBy(_.id)
      .process( new TempIncreAlert() )

    val processedStream2 = dataStream.keyBy(_.id)
//      .process( new TempChangeAlert(10.0) )
      .flatMap( new TempChangeAlert(10.0) )

    val processedStream3 = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]{
      // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
      case ( input: SensorReading, None ) => ( List.empty, Some(input.temperature) )
      // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
      case ( input: SensorReading, lastTemp: Some[Double] ) =>
        val diff = ( input.temperature - lastTemp.get ).abs
        if( diff > 10.0 ){
          ( List((input.id, lastTemp.get, input.temperature)), Some(input.temperature) )
        } else
          ( List.empty, Some(input.temperature) )
    }

    dataStream.print("input data")
    processedStream3.print("processed data")

    env.execute("process function test")
  }
}

class TempIncreAlert() extends KeyedProcessFunction[String,SensorReading , String]{

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("currentTimer", classOf[Long]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update( value.temperature )

    val curTimerTs = currentTimer.value()


    if( value.temperature < preTemp || preTemp == 0.0 ){
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer( curTimerTs )
      currentTimer.clear()
    } else if ( value.temperature > preTemp && curTimerTs == 0 ){
      // 温度上升且没有设过定时器，则注册定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 5000L
      ctx.timerService().registerProcessingTimeTimer( timerTs )
      currentTimer.update( timerTs )
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect( ctx.getCurrentKey + " 温度连续上升" )
    currentTimer.clear()
  }
}

class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if(diff > threshold){
      out.collect( (value.id, lastTemp, value.temperature) )
    }
    lastTempState.update(value.temperature)
  }

}


class TempChangeAlert2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{
  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if(diff > threshold){
      out.collect( (value.id, lastTemp, value.temperature) )
    }
    lastTempState.update(value.temperature)
  }
}