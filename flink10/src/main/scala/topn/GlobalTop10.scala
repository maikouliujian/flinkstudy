package topn

import java.util
import java.util.{Comparator, Properties}

import org.apache.flink.api.common.functions.{CoGroupFunction, ReduceFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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

/** *
  * 在上一篇Flink实战: 窗口TopN分析与实现中实现了在一个窗口内的分组topN，
  * 但是在实际中也会遇到没有窗口期的topN,例如在一些实时大屏监控展示中，展示历史到现在所有的TopN数据，将这个称之为全局topN,仍然以计算区域维度销售额topN的商品为例，看一下全局TopN的实现方法。
  * 先将需求分解为以下几步：
  * *
  * 按照区域areaId+商品gdsId分组，计算每个分组的累计销售额
  * *
  * 将得到的区域areaId+商品gdsId维度的销售额按照区域areaId分组，然后求得TopN的销售额商品，并且定时更新输出
  * *
  * 与窗口TopN不同，全局TopN没有时间窗口的概念，也就没有时间的概念，因此使用ProcessingTime语义即可，并且也不能再使用Window算子来操作，但是在这个过程中需要完成数据累加操作与定时输出功能，选择ProcessFunction函数来完成，
  * 使用State保存中间结果数据，保证数据一致性语义，使用定时器来完成定时输出功能。
  *
  */

import org.apache.flink.api.scala._

case class Order2(orderId: String, orderTime: Long, gdsId: String, amount: Double, areaId: String)
case class GdsSales(areaId: String,gdsId: String,amount: Double, orderTime: Long)

object GlobalTop10 {

  def main(args: Array[String]): Unit = {
    val N = 10
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")

    val consumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), kafkaConfig)

    val orderStream = env.addSource(consumer)
      .map(x => {
        val a = x.split(",")
        Order2(a(0), a(1).toLong, a(2), a(3).toDouble, a(4))
      })

    val salesStream = orderStream.keyBy(x => {
      x.areaId + "_" + x.gdsId
    }).process(new KeyedProcessFunction[String, Order2, GdsSales]() {

      var orderState: ValueState[Double] = _
      var orderStateDesc: ValueStateDescriptor[Double] = _

      override def open(parameters: Configuration): Unit = {
        orderStateDesc = new ValueStateDescriptor[Double]("order-state", TypeInformation.of(classOf[Double]))
        orderState = getRuntimeContext.getState(orderStateDesc)

      }

      override def processElement(value: Order2, ctx: KeyedProcessFunction[String, Order2, GdsSales]#Context, out: Collector[GdsSales]): Unit = {


        val currV = orderState.value()
        if (currV == null) {
          orderState.update(value.amount)
        } else {
          val newV = currV + value.amount
          orderState.update(newV)
        }
        out.collect(GdsSales(value.areaId, value.gdsId, orderState.value(), value.orderTime))
      }
    })

    /***
      * 上一步得到的salesStream是一个按照区域areaId+商品gdsId维度的销售额，并且是不断更新输出到下游的，接下来就需要完成TopN的计算，
      * 在Flink实战: 窗口TopN分析与实现中分析到TopN的计算不需要保存所有的结果数据，使用红黑树来模拟类似优先级队列功能即可，但是与其不同在于：
      * 窗口TopN每次计算TopN是一个全量的窗口结果，而全局TopN其销售额是会不断变动的，因此需要做以下逻辑判断：

    如果TreeSet[GdsSales]包含该商品的销售额数据，则需要更新该商品销售额，这个过程包含判断商品gdsId是否存在与移除该GdsSales对象功能，
    但是TreeSet不具备直接判断gdsId是否存在功能，那么可以使用一种额外的数据结构Map, key为商品gdsId, value为商品销售额数据GdsSales，
    该value对应TreeSet[GdsSales]中数据

    如果TreeSet[GdsSales]包含该商品的销售额数据，当TreeSet里面的数据到达N, 就获取第一个节点数据(最小值)与当前需要插入的数据进行比较，
    如果比其大，则直接舍弃，如果比其小，那么就将TreeSet中第一个节点数据删除，插入新的数据
    实现代码如下：
      */
    salesStream.keyBy(_.areaId)
      .process(new KeyedProcessFunction[String, GdsSales, Void] {

        var topState: ValueState[java.util.TreeSet[GdsSales]] = _
        var topStateDesc: ValueStateDescriptor[java.util.TreeSet[GdsSales]] = _
        var mappingState: MapState[String, GdsSales] = _
        var mappingStateDesc: MapStateDescriptor[String, GdsSales] = _

        val interval: Long = 60000
        val N: Int = 3

        //定期发送
        var fireState:ValueState[Long]= _
        var fireStateDesc:ValueStateDescriptor[Long]= _

        override def open(parameters: Configuration): Unit = {
          topStateDesc = new ValueStateDescriptor[java.util.TreeSet[GdsSales]]("top-state", TypeInformation.of(classOf[java.util.TreeSet[GdsSales]]))
          topState = getRuntimeContext.getState(topStateDesc)
          mappingStateDesc = new MapStateDescriptor[String, GdsSales]("mapping-state", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[GdsSales]))
          mappingState = getRuntimeContext.getMapState(mappingStateDesc)

          fireStateDesc =new ValueStateDescriptor[Long]("fire-time",TypeInformation.of(classOf[Long]))
          fireState = getRuntimeContext.getState(fireStateDesc)

        }

        override def processElement(value: GdsSales, ctx: KeyedProcessFunction[String, GdsSales, Void]#Context, out: Collector[Void]): Unit = {
          val top = topState.value()
          if (top == null) {
            val topMap: util.TreeSet[GdsSales] = new util.TreeSet[GdsSales](new Comparator[GdsSales] {
              override def compare(o1: GdsSales, o2: GdsSales): Int = (o1.amount - o2.amount).toInt
            })

            topMap.add(value)
            topState.update(topMap)
            mappingState.put(value.gdsId, value)
          } else {
            mappingState.contains(value.gdsId) match {
              case true => {
                //已经存在该商品的销售数据
                val oldV = mappingState.get(value.gdsId)
                mappingState.put(value.gdsId, value)
                //去除红黑树
                val values = topState.value()

                values.remove(oldV)
                values.add(value) //更新旧的商品销售数据
                topState.update(values)

              }
              case false => {
                //不存在该商品销售数据
                if (top.size() >= N) {
                  //已经达到N 则判断更新
                  val min = top.first()
                  if (value.amount > min.amount) {
                    top.pollFirst()
                    top.add(value)
                    mappingState.put(value.gdsId, value)
                    topState.update(top)
                  }
                } else {
                  //还未到达N则直接插入
                  top.add(value)
                  mappingState.put(value.gdsId, value)
                  topState.update(top)
                }
              }
            }
          }
          val currTime = ctx.timerService().currentProcessingTime()
          //1min输出一次
          //第一次注册时间
          if(fireState.value()==null){
            val start = currTime -(currTime % interval)
            val nextFireTimestamp = start + interval
            ctx.timerService().registerProcessingTimeTimer(nextFireTimestamp)
            fireState.update(nextFireTimestamp)
          }
        }

        //触发定期发送逻辑
        override def onTimer(timestamp:Long, ctx:KeyedProcessFunction[String,GdsSales,Void]#OnTimerContext,out:Collector[Void]):Unit={
          println(timestamp +"===")
          import scala.collection.JavaConverters._
          topState.value().asScala.foreach(x =>{
            println(x)
          })

          val fireTimestamp = fireState.value()
          if(fireTimestamp !=null&&(fireTimestamp == timestamp)){
            fireState.clear()
            fireState.update(timestamp + interval)
            //定期注册时间
            ctx.timerService().registerProcessingTimeTimer(timestamp + interval)
          }

        }
      })
  }
}
