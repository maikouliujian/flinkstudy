package asyncio

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig


/***
  * 以上就是简易版使用广播状态来实现维表关联的实现，由于将维表数据存储在广播状态中，但是广播状态是非key的，
  * 而rocksdb类型statebackend只能存储keyed状态类型，所以广播维表数据只能存储在内存中，因此在使用中需要注意维表的大小以免撑爆内存。
 *
 * https://blog.csdn.net/weixin_47364682/article/details/106149996
  * @param actionType
  * @param b
  */
case class Rule(actionType:String,b:Boolean)
case class UserAction(userId:String,actionType:String,time:String)

object Demo3 {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)

    val kafkaConfig = new Properties();
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

    import org.apache.flink.api.scala._

    val ruleConsumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), kafkaConfig)
    val ruleStream = env.addSource(ruleConsumer)
      .map(x => {
        val a = x.split(",")
        Rule(a(0), a(1).toBoolean)
      })

    val broadcastStateDesc = new MapStateDescriptor[String, Rule]("broadcast-state", BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[Rule] {}))

    val broadcastRuleStream = ruleStream.broadcast(broadcastStateDesc)


    val userActionConsumer = new FlinkKafkaConsumer011[String]("topic2", new SimpleStringSchema(), kafkaConfig)
    val userActionStream = env.addSource(userActionConsumer).map(x => {
      val a = x.split(",")
      UserAction(a(0), a(1), a(2))
    }).keyBy(_.userId)

    val connectedStream = userActionStream.connect(broadcastRuleStream)
    connectedStream.process(new KeyedBroadcastProcessFunction[String, UserAction, Rule, String] {

      override def processElement(value: UserAction, ctx: KeyedBroadcastProcessFunction[String, UserAction, Rule, String]#ReadOnlyContext,
                                  out: Collector[String]): Unit = {
        val state = ctx.getBroadcastState(broadcastStateDesc)
        if (state.contains(value.actionType)) {
          out.collect(Tuple4.apply(value.userId, value.actionType, value.time, "true").toString())
        }
      }

      //这里可以把最新传来的广播变量存储起来，processElement中可以取出再次使用
      /***
       * 最后还有一点需要注意，processElement()方法获取的Context实例是ReadOnlyContext，说明只有在广播流一侧才能修改BroadcastState，
       * 而数据流一侧只能读取BroadcastState。这提供了非常重要的一致性保证：假如数据流一侧也能修改BroadcastState的话，
       * 不同的operator实例有可能产生截然不同的结果，对下游处理造成困扰。
       * @param value
       * @param ctx
       * @param out
       */
      override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[String, UserAction, Rule, String]#Context,
                                           out: Collector[String]): Unit = {
        ctx.getBroadcastState(broadcastStateDesc).put(value.actionType, value)
      }
    })

    env.execute()
  }

}
