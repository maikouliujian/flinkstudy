package stream.transform

import org.apache.flink.streaming.api.scala._
import stream.state.SensorReading

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

    //3、分流
    val value: SplitStream[SensorReading] = stream2.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val high: DataStream[SensorReading] = value.select("high")
    val low = value.select("low")
    val all = value.select("high","low")

    //stream2.getSideOutput()

    val warning = high.map( sensorData => (sensorData.id,
      sensorData.temperature) )
    val connected = warning.connect(low)
    val coMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

   //合并以后打印
    val  unionStream: DataStream[SensorReading] = low.union(high)
    unionStream.print("union:::")

    unionStream.print()
    env.execute()

  }

  }


