package stream.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import stream.source.SourceTest.SensorReading

object EsSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream2 = env.readTextFile("D:\\idea\\project\\flinkstudy\\data\\sensor.txt")
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
          dataArray(2).trim.toDouble)
      })

    val httpHosts: util.List[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    val  esSinkBuilder =  new  ElasticsearchSink.Builder[SensorReading]( httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          val json = new util.HashMap[String, String]()
          json.put("data", t.toString)
          val indexRequest =
            Requests.indexRequest().index("sensor").`type`("readingData").source(json)
          requestIndexer.add(indexRequest)
          println("saved successfully")
        }
      } )
    stream2.addSink( esSinkBuilder.build() )

  }

}


