package stream.transform

import org.apache.flink.streaming.api.scala._
import stream.source.SourceTest.SensorReading

object Transform {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream2 = env.readTextFile("D:\\idea\\project\\flinkstudy\\data\\sensor.txt")
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
          dataArray(2).trim.toDouble)
      })
      .keyBy("id")
      //x代表同一分区的上一条数据，y代表当前这条数据
      .reduce( (x, y) =>  SensorReading(x.id, x.timestamp +  1, y.temperature +10) )


    stream2.split()

    stream2.print()
    env.execute()

  }

  }


