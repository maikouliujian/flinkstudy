package topn

import java.util
import java.util.{Comparator, Properties}

import org.apache.flink.api.common.functions.{CoGroupFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/***
  * TopN 的需求场景不管是在离线计算还是实时计算都是比较常见的，例如电商中计算热门销售商品、广告计算中点击数前N的广告、
  * 搜索中计算搜索次数前N的搜索词。topN又分为全局topN、分组topN, 比喻说热门销售商品可以直接按照各个商品的销售总额排序，
  * 也可以先按照地域分组然后对各个地域下各个商品的销售总额排序。本篇以热门销售商品为例，实时统计每10min内各个地域维度下销售额top10的商品。

这个需求可以分解为以下几个步骤：

    提取数据中订单时间为事件时间

    按照区域+商品为维度，统计每隔10min中的销售额

    按照区域为维度，统计该区域的top10 销售额的商品

  https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483990&idx=1&sn=9e3f05575191dffa9cd018cfab454bb8&chksm=fe2b6619c95cef0f676319683c735247e9c9e57cafd66a132f9a9f53ee39aed64d6dacd27ffd&scene=21#wechat_redirect

  */
import org.apache.flink.api.scala._
case class Order2(orderId: String, orderTime: Long, gdsId: String, amount: Double, areaId: String)

object Top10{

  def main(args:Array[String]):Unit={
    val N = 10
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val kafkaConfig =new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")
    val orderConsumer =new FlinkKafkaConsumer011[String]("topic1",new SimpleStringSchema, kafkaConfig)

    val ds = env.addSource(orderConsumer)
      .map(x =>{
        val a = x.split(",")
        Order2(a(0), a(1).toLong, a(2),a(3).toDouble,a(4))
      })

    //我们这里统计的每10min内的数据，希望按照真实的订单时间统计，
    // 那么使用事件时间EventTime,考虑到可能存在数据乱序问题，允许最大延时为30s

    val dataStream=ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order2](Time.seconds(30)) {
      override def extractTimestamp(element: Order2): Long = element.orderTime

    })

    //统计每十分钟里，以区域和商品id为key的销售额
    val amountStream=dataStream.keyBy(x => {
      //以区域和商品id为key
      x.areaId + "_" + x.gdsId
    }).timeWindow(Time.minutes(10))

      .reduce(new ReduceFunction[Order2] {
        override def reduce(value1: Order2, value2: Order2): Order2 = {
          Order2(value1.orderId, value1.orderTime, value1.gdsId, value1.amount + value2.amount, value1.areaId)

        }

      })

    //按照区域id划分，取金额top10
    //到目前为止已经拿到了每个10min内各个区域下的各个商品的销售额amountStream，
    // 现在需要对其按照区域为维度分组，计算top10销售额的商品，需要考虑两个问题：

    //如何获取到10min窗口的所有数据

   // 如何排序

    //先看第一个如何获取到10min窗口的数据，也就是amountStream的每个窗口的输出，
    // 这个其实在Flink 官网上也给出了说明，那么就是直接在后面接一个相同大小的窗口即可，那么后面的窗口即获取到了前一个窗口的所有数据，代码如下：
    val value: WindowedStream[Order2, String, windows.TimeWindow] = amountStream.keyBy(_.areaId)
      .timeWindow(Time.minutes(10))
    value.apply(new WindowFunction[Order2, Order2, String, windows.TimeWindow] {

      override def apply(key: String, window: TimeWindow, input: Iterable[Order2], out: Collector[Order2]): Unit = {
        println("==area===" + key)
        val topMap = new util.TreeSet[Order2](new Comparator[Order2] {
          override def compare(o1: Order2, o2: Order2): Int = (o1.amount-o2.amount).toInt
        })
        import scala.collection.JavaConverters._
        input.foreach(x => {
          if (topMap.size() >= N) {
            //取出treeset中最小key
            val min=topMap.first()
            if(x.amount>min.amount) {
              topMap.pollFirst() //舍弃
              topMap.add(x)
            }
          }else{
            topMap.add(x)
          }
        })

        //这里直接做打印
        topMap.asScala.foreach(x=>{
          println(x)
        })
      }
    })


    env.execute()

  }}
