package stream.tables

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import org.apache.flink.api.scala._

object TableTest {

  //每 10 秒中渠道为 appstore 的个数
  def  main(args:  Array[String]):  Unit  =  {
    //sparkcontext
//    val  env:  StreamExecutionEnvironment  = StreamExecutionEnvironment.getExecutionEnvironment
//    //时间特性改为 eventTime
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val  myKafkaConsumer:  FlinkKafkaConsumer011[String]  = MyKafkaUtil.getConsumer("ECOMMERCE")
//    val  dstream:  DataStream[String]  =  env.addSource(myKafkaConsumer)
//    val ecommerceLogDstream: DataStream[EcommerceLog] = dstream.map{ jsonString
//    =>JSON.parseObject(jsonString,classOf[EcommerceLog])  }
//    //告知 watermark  和 eventTime 如何提取
//    val  ecommerceLogWithEventTimeDStream:  DataStream[EcommerceLog]  = ecommerceLogDstream.
//      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[EcommerceLog](Time.seconds(0L))  {
//      override  def  extractTimestamp(element:  EcommerceLog):  Long  =  { element.ts
//      }
//    }).setParallelism(1)
//    val  tableEnv:  StreamTableEnvironment  = TableEnvironment.getTableEnvironment(env)
//    //把数据流转化成 Table
//    val  ecommerceTable:  Table  = tableEnv.fromDataStream(ecommerceLogWithEventTimeDStream  , 'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinut e,'ts.rowtime)
//    //通过 table  api  进行操作
//    // 每 10 秒 统计一次各个渠道的个数 table api 解决
//    //1  groupby   2  要用 window     3  用 eventtime 来确定开窗时间
//    val  resultTable:  Table  =  ecommerceTable.window(Tumble  over  10000.millis  on
//      'ts  as  'tt).groupBy('ch,'tt  ).select(  'ch,  'ch.count)
//    //把 Table 转化成数据流
//    val  resultDstream:  DataStream[(Boolean,  (String,  Long))]  = resultSQLTable.toRetractStream[(String,Long)]
//    resultDstream.filter(_._1).print()
//    env.execute()
  }

}
