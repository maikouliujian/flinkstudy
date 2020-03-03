package flinksql.udaf

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig


/***
  * 场景案例

先从一个实际业务场景理解Flink SQL中的撤回机制：设备状态上线/下线数量统计，上游采集设备状态发送到Kafka中，最开始是一个上线状态，此时统计到上线数量+1，过了一段时间该设备下线了，收到的下线的状态，
那么此时应该是上线数量-1，下线数量+1，现在需要实现这样一个需求，看一下在Flink SQL里面如何实现
  * @param a
  * @param b
  * @param times
  */
case class DevData(a:String,b:Int,times:Long)
object Anli1 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //    2. 获取Table运行环境
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv = StreamTableEnvironment.create(env,bsSettings)

    tabEnv.registerFunction("latestTimeUdf",new LatestTimeUdf())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val kafkaConfig=new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")


    val consumer=new FlinkKafkaConsumer011[String]("topic1",new SimpleStringSchema,kafkaConfig)

    val ds=env.addSource(consumer)
      .map(x=>{
        val a=x.split(",")
        DevData(a(0),a(1).toInt,a(2).toLong)

      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DevData](Time.milliseconds(1000)){

      override def extractTimestamp(element:DevData):Long= element.times

    })

    //  7. 导入`import org.apache.flink.table.api.scala._`隐式参数
    import org.apache.flink.table.api.scala._
    //将数据流注册为table
    tabEnv.registerDataStream("tbl1",ds,'devId,'status,'times,'times.rowtime)

    val dw=tabEnv.sqlQuery(

      """
            select st,count(*) from (
                    select latestTimeUdf(status,times) st,devId from tbl1 group by devId
                    ) a group by st
          """.stripMargin)

    //dw.writeToSink(new PaulRetractStreamTableSink)
    //dw.addSink()
    env.execute()

  }
}


