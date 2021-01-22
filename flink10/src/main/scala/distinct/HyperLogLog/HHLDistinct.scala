package distinct.HyperLogLog

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig

case class AdData(id:Int,devId:String,time:Long)
case class AdKey(id:Int,time:Long)

/***
  * 计算每个广告每小时的点击用户数，广告点击日志包含：广告位ID、用户设备ID(idfa/imei/cookie)、点击时间。
  */
object HHLDistinct {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv=StreamTableEnvironment.create(env)
    import org.apache.flink.api.scala._
    //注册udaf
    tabEnv.registerFunction("hllDistinct",new HLLDistinctFunction)
    val kafkaConfig=new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")
    val consumer=new FlinkKafkaConsumer[String]("topic1",new SimpleStringSchema,kafkaConfig)
    consumer.setStartFromLatest()
    val ds=env.addSource(consumer)
      .map(x=>{
        val s=x.split(",")
        AdData(s(0).toInt,s(1),s(2).toLong)
      })
    //tabEnv.registerDataStream("pv",ds)
    tabEnv.createTemporaryView("pv",ds)
    val rs=tabEnv.sqlQuery(      """ select hllDistinct(devId) ,datatime
                                          from pv group by datatime
      """.stripMargin)
    //rs.writeToSink(new PaulRetractStreamTableSink)
    env.execute()
  }

}
