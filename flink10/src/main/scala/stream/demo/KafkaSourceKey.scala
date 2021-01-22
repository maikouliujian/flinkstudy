package stream.demo

import common.Common
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author lj
  * @createDate 2019/12/27 11:58
  **/

import org.apache.flink.api.scala._
object KafkaSourceKey {


  def main(args: Array[String]): Unit = {
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new RichSourceFunction[String] {
      // kafka consumer 对象
      var consumer: KafkaConsumer[String, String] = null

      // 初始化方法
      override def open(parameters: Configuration): Unit = {
        consumer = new KafkaConsumer[String, String](Common.prop)
        // 订阅topic
        val list = List("kafka_key")
        //TODO    scala和java集合互相转换！！！
        import scala.collection.JavaConversions._
        consumer.subscribe(list)
      }
      // 执行方法，拉取数据，获取到的数据，会放到source 的缓冲区
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        println("run")
        while (true) {
          val records = consumer.poll(500)
          val tmp = records.iterator()
          while (tmp.hasNext) {
            val record = tmp.next()
            val key = record.key()
            val value = record.value()

            ctx.collect("key : " + key + ", value " + value)
          }
        }

      }

      override def cancel(): Unit = {

        println("cancel")
      }
    }).map(s => s + "map")
      .addSink(new RichSinkFunction[String] {
        // kafka producer 对象
        var producer: KafkaProducer[String, String] = null

        // 初始化
        override def open(parameters: Configuration): Unit = {
          producer = new KafkaProducer[String, String](Common.getProp)
        }

        override def close(): Unit = {
          if (producer == null) {
            producer.flush()
            producer.close()
          }
        }

        // 输出数据，每条结果都会执行一次，并发高的时候，可以按需做flush
        override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

          println("flink : " + value)

          val msg = new ProducerRecord[String, String]( "kafka_key_out", "key" + System.currentTimeMillis(), value)
          producer.send(msg)
          producer.flush()
        }
      })
    // execute job
    env.execute("KafkaToKafka")
  }


}
