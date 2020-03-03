package flinksql.udtf

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.google.gson.JsonArray
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

表函数TableFunction相对标量函数ScalarFunction一对一，它是一个一对多的情况，通常使用TableFunction来完成列转行的一个操作。
先通过一个实际案例了解其用法：终端设备上报数据，数据类型包含温度、耗电量等，上报方式是以多条方式上报，例如：
  * @param a
  * @param b
  * @param times
  */

case class RawData(devId:String,time:Long,data:String)
object Anli2 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //    2. 获取Table运行环境
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv = StreamTableEnvironment.create(env,bsSettings)
    import org.apache.flink.api.scala._
    tabEnv.registerFunction("udtf",new MyUDTF)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val kafkaConfig=new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")


    val consumer=new FlinkKafkaConsumer011[String]("topic1",new SimpleStringSchema,kafkaConfig)

    val ds=env.addSource(consumer)
      .map(x=>{
        val obj = JSON.parseObject(x, classOf[RawData])
        Tuple3.apply(obj.devId, obj.time, obj.data)

      })

    //  7. 导入`import org.apache.flink.table.api.scala._`隐式参数
    import org.apache.flink.table.api.scala._
    //将数据流注册为table
    tabEnv.registerDataStream("tbl1", ds,'devId, 'time,'data)

    /***
      * 至此拿到了符合要求的数据。在Flink SQL中使用TableFunction需要搭配LATERAL TABLE一起使用，将其认为是一张虚拟的表，
      * 整个过程就是一个Join with Table Function过程，左表(tbl1) 会join 右表(t1) 的每一条记录。
      * 但是也存在另外一种情况右表(t1)没有输出但是也需要左表输出那么可以使用LEFT JOIN LATERAL TABLE，用法如下：
      *
      *     SELECT users, tag
    FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) t AS tag ON TRUE
      */

    val rsTab = tabEnv.sqlQuery("select devId,`time`,`type`,`value` from tbl1 , LATERAL TABLE(udtf(data)) as t(`type`,`value`) ")

    //dw.writeToSink(new PaulRetractStreamTableSink)
    //dw.addSink()
    env.execute()

  }
}


