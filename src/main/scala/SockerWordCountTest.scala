import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object SockerWordCountTest {

  def main(args: Array[String]): Unit = {

    //val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("192.168.40.130",2222)
    import org.apache.flink.api.scala._

    val flatmap: DataStream[String] = stream.flatMap(_.split("\\s"))
    val map: DataStream[(String, Int)] = flatmap.map((_,1))
    val keyby: KeyedStream[(String, Int), Tuple] = map.keyBy(0)
    val window: WindowedStream[(String, Int), Tuple, TimeWindow] = keyby.timeWindow(Time.seconds(2))
    val result: DataStream[(String, Int)] = window.sum(1)

    result.print().setParallelism(4)

    env.execute("test")



  }

}
