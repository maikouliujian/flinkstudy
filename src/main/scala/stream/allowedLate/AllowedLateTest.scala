package stream.allowedLate

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * @author lj
  * @createDate 2019/12/26 11:14
  **/
object AllowedLateTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[(Int, Int)] = env.fromCollection(List((1, 2), (3, 4), (1, 3)))

    //todo 定义接收延迟数据的
    val lateOutData: OutputTag[(Int, Int)] = OutputTag[(Int, Int)]("lateDate")
    val stream = dataStream.keyBy(_._1)
      //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(5)) //允许延迟5秒
      .sideOutputLateData(lateOutData)
      .process(new ProcessWindowFunction[(Int, Int),(Int, Int),Int,TimeWindow] {
        override def process(key: Int, context: Context,
                             elements: Iterable[(Int, Int)],
                             out: Collector[(Int, Int)]): Unit = {
          out.collect((1,2))
        }
      })

    //拿到延迟的数据，可以做相应的操作逻辑
    val lateDatas = stream.getSideOutput(lateOutData)

    env.execute()

  }

}
