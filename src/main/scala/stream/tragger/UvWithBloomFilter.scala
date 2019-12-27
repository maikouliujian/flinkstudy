package stream.tragger

import javafx.scene.effect.Bloom
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @author lj
  * @createDate 2019/12/25 12:12
  **/
import org.apache.flink.api.scala._

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String,
                        timestamp: Long)

case class UvCount(windowEnd: Long, count: Long)


object UvWithBloomFilter {

  def main(args: Array[String]): Unit = {

    //val resourcesPath = getClass.getResource("./UserBehavior.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val windowdata: WindowedStream[(String, Long), String, TimeWindow] = env.
      readTextFile("D:\\work\\ideaproject\\UserBehaviorAnalysisFlink\\NetWorkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong,
          dataArray(1).toLong,
          dataArray(2).toInt,
          dataArray(3),
          dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60 * 60))
    val stream: DataStream[UvCount] = windowdata.trigger(new MyTrigger()) //自定义触发器
      .process(new UvCountWithBloom())//自定义窗口处理函数
    stream.print("bloom")

    env.execute("Unique Visitor with bloom Job")




  }

}

//自定义窗口触发器
class MyTrigger extends Trigger[(String,Long),TimeWindow] {
  override def onElement(element: (String, Long),
                         timestamp: Long, window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult ={
    // 每来一条数据，就触发窗口操作并清空
    TriggerResult.FIRE_AND_PURGE

  }

  override def onProcessingTime(time: Long, window:
  TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window:
  TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow,
                     ctx: Trigger.TriggerContext): Unit = {}
}
// 自定义窗口处理函数

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String,
  TimeWindow] {

  // 创建 redis连接
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[UvCount]): Unit = {

    val storeKey = context.window.getEnd.toString
    var count = 0L

    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    val userId = elements.last._2.toString

    val offset = bloom.hash(userId, 61)

    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      jedis.setbit(storeKey, offset, true)

      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }

  }
}


// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      // 最简单的 hash算法，每一位字符的 ascii码值，乘以 seed之后，做叠加
      result = result * seed + value.charAt(i)

    }
    (cap - 1) & result
  }
}
