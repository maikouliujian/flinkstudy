package distinct

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class Distinct1ProcessFunction extends KeyedProcessFunction[AdKey, AdData, Void] {

  var devIdState: MapState[String, Int] = _
  var devIdStateDesc: MapStateDescriptor[String, Int] = _
  var countState: ValueState[Long] = _
  var countStateDesc: ValueStateDescriptor[Long] = _


  override def open(parameters: Configuration): Unit = {
    devIdStateDesc = new MapStateDescriptor[String, Int]("devIdState", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Int]))
    devIdState = getRuntimeContext.getMapState(devIdStateDesc)
    countStateDesc = new ValueStateDescriptor[Long]("countState", TypeInformation.of(classOf[Long]))
    countState = getRuntimeContext.getState(countStateDesc)

  }


  override def processElement(value: AdData, ctx: KeyedProcessFunction[AdKey, AdData, Void]#Context, out: Collector[Void]): Unit = {
    //主要考虑可能会存在滞后的数据比较严重，会影响之前的计算结果，
    // 做了一个类似window机制里面的一个延时判断，将延时的数据过滤掉，也可以使用OutputTag 单独处理。
    val currW=ctx.timerService().currentWatermark()
    if(ctx.getCurrentKey.time+1<=currW) {
      println("late data:" + value)
      return
    }


    val devId = value.devId
    devIdState.get(devId) match {
      case 1 => {
        //表示已经存在
      }

      case _ => {
        //表示不存在
        devIdState.put(devId, 1)
        val c = countState.value()
        countState.update(c + 1)
        //还需要注册一个定时器
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
      }
    }
    println(countState.value())
  }

//数据清理通过注册定时器方式ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
  // 表示当watermark大于该小时结束时间+1就会执行清理动作，调用onTimer方法。

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[AdKey, AdData, Void]#OnTimerContext, out: Collector[Void]): Unit = {
    println(timestamp + " exec clean~~~")
    println(countState.value())
    devIdState.clear()
    countState.clear()
  }

}
