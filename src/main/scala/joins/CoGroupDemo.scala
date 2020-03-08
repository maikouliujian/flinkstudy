package joins

import java.util.Properties

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

case class Order(id:String, gdsId:String, amount:Double)
case class Gds(id:String, name:String)
case class RsInfo(orderId:String, gdsId:String, amount:Double, gdsName:String)

object CoGroupDemo{

  def main(args:Array[String]):Unit={
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val kafkaConfig =new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")
    val orderConsumer =new FlinkKafkaConsumer011[String]("topic1",new SimpleStringSchema, kafkaConfig)
    val gdsConsumer =new FlinkKafkaConsumer011[String]("topic2",new SimpleStringSchema, kafkaConfig)
    val orderDs = env.addSource(orderConsumer)
      .map(x =>{
        val a = x.split(",")
        Order(a(0), a(1), a(2).toDouble)
      })

    val gdsDs = env.addSource(gdsConsumer)
      .map(x =>{
        val a = x.split(",")
        Gds(a(0), a(1))
      })


    orderDs.coGroup(gdsDs)
      .where(_.gdsId)// orderDs 中选择key
      .equalTo(_.id)//gdsDs中选择key
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .apply(new CoGroupFunction[Order,Gds,RsInfo]{
        override def coGroup(first: java.lang.Iterable[Order], second: java.lang.Iterable[Gds],out:Collector[RsInfo]):Unit={
          //得到两个流中相同key的集合
          //Left/Right join实现分析
          import scala.collection.JavaConverters._
          first.asScala.foreach(x =>{
            if(second.asScala.nonEmpty){
              second.asScala.foreach(y=>{
                out.collect(RsInfo(x.id,x.gdsId,x.amount,y.name))
              })
            }
            if(second.asScala.isEmpty){
              out.collect(RsInfo(x.id,x.gdsId,x.amount,null))
            }
          })
        }
      })

    env.execute()
  }}
