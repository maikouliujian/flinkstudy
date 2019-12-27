package stream.tragger

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger

/**
  * @author lj
  * @createDate 2019/12/26 11:07
  **/

import org.apache.flink.api.scala._
object TreiggerDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[(Int, Int)] = env.fromCollection(List((1, 2), (3, 4), (1, 3)))
    dataStream.keyBy(_._1)
      //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
      .timeWindow(Time.seconds(10))
      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))

    env.execute()
  }


}
