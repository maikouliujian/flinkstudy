package demo

import common.Common
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.{MathUtil, StringUtil}

import scala.reflect.io.File

/**
 * @author : 刘剑
 * @date : 2020/12/25 8:49 下午
 * @description
 */
object WordCountDistinct {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    if ("/".equals(File.separator)) {
//      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
//      env.setStateBackend(backend)
//      env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)
//    } else {
//      env.setMaxParallelism(1)
//      env.setParallelism(1)
//    }
    import org.apache.flink.api.scala._

    val input = env.addSource(new RadomFunction)
      .map(s => {
        val tmp = s.split(",")
        (tmp(0), tmp(1))
      })
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      //用触发器是因为窗口很大，只能在窗口结束时触发计算，看到一次结果，这不满足需求
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
      //使用驱逐器是因为窗口很大，
      // 1、每次触发计算是触发窗口内所有的元素，不清除会重复计算；2、如果不清除，窗口中的元素越来越多，会引起oom；
      // 3、因为在触发计算时间，已经把当前的所有元素的中间结果存到了state中，
      .evictor(TimeEvictor.of(Time.seconds(0), true))
      .process(new ProcessWindowFunction[(String, String), (String, String, Long), Tuple, TimeWindow] {
        /*
        这是使用state是因为，窗口默认只会在创建结束的时候触发一次计算，然后数据结果，
        如果长时间的窗口，比如：一天的窗口，要是等到一天结束在输出结果，那还不如跑批。
        所有大窗口会添加trigger，以一定的频率输出中间结果。
        加evictor 是因为，每次trigger，触发计算是，窗口中的所有数据都会参与，所以数据会触发很多次，比较浪费，加evictor 驱逐已经计算过的数据，就不会重复计算了
        驱逐了已经计算过的数据，导致窗口数据不完全，所以需要state 存储我们需要的中间结果，清除之前的元素并不影响结果计算
         */
        var wordState: MapState[String, String] = _
        var pvCount: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // new MapStateDescriptor[String, String]("word", classOf[String], classOf[String])
          wordState = getRuntimeContext.getMapState(new MapStateDescriptor[String, String]("word", classOf[String], classOf[String]))
          pvCount = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("pvCount", classOf[Long]))
        }

        override def process(key: Tuple, context: Context, elements: Iterable[(String, String)], out: Collector[(String, String, Long)]): Unit = {


          var pv = 0;
          val elementsIterator = elements.iterator
          // 遍历窗口数据，获取唯一word
          while (elementsIterator.hasNext) {
            pv += 1
            val word = elementsIterator.next()._2
            wordState.put(word, null)
          }
          // add current
          pvCount.update(pvCount.value() + pv)
          var count: Long = 0
          val wordIterator = wordState.keys().iterator()
          while (wordIterator.hasNext) {
            wordIterator.next()
            count += 1
          }
          // uv
          out.collect((key.getField(0), "uv", count))
          // fix bug: collect pvCount value in state
          out.collect(key.getField(0), "pv", pvCount.value())

        }
      })
      .print()
    /* 結果
(u,uv,275)
(u,pv,321)
(o,uv,274)
(o,pv,316)
(P,uv,278)
(P,pv,325)
     */

    env.execute("WordCountDistinct")
  }
}

class RadomFunction extends SourceFunction[String] {
  var flag = true

  override def cancel(): Unit = {
    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (flag) {
      // type,user
      /*
      q,123
      z,456
       */
      ctx.collect(StringUtil.getRandomString(1) + "," + MathUtil.getRadomNum(3))
      Thread.sleep(1)
    }
  }
}
