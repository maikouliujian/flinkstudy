package distinct

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

case class AdData(id:Int,devId:String,time:Long)
case class AdKey(id:Int,time:Long)

/***
  * 计算每个广告每小时的点击用户数，广告点击日志包含：广告位ID、用户设备ID(idfa/imei/cookie)、点击时间。
  */
object Distinct1 {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig=new Properties()

    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")

    val consumer=new FlinkKafkaConsumer[String]("topic1",new SimpleStringSchema,kafkaConfig)

    val ds=env.addSource(consumer)
      .map(x=>{
        val s=x.split(",")
        AdData(s(0).toInt,s(1),s(2).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdData](Time.minutes(1)) {
      override def extractTimestamp(element: AdData): Long = element.time

    })
      //数据分组使用广告位ID+点击事件所属的小时
      .keyBy(x=>{
        //获取x.time所对应的的窗口【一小时窗口】的结束日期
        val endTime= TimeWindow.getWindowStartWithOffset(x.time, 0,
          Time.hours(1).toMilliseconds) + Time.hours(1).toMilliseconds
        AdKey(x.id,endTime)

      }).process(new Distinct1ProcessFunction)
  }

}
