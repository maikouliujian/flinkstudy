package stream.state

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 11:41
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读入数据
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // Transform操作

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )

    // 1. 聚合操作
     val stream1 = dataStream
       .keyBy("id")
//      .sum("temperature")
      .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10) )

    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream
      .split( sensorData => {
        if( sensorData.temperature > 30 ) Seq("high") else Seq("low")
      } )

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合并两条流
    val warningStream = highTempStream.map( sensorData => (sensorData.id, sensorData.temperature) )
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => ( warningData._1, warningData._2, "high temperature warning" ),
      lowData => ( lowData.id, "healthy" )
    )

    val unionStream = highTempStream.union(lowTempStream)

    // 函数类
    dataStream.filter( new MyFilter() ).print()

    // 输出数据
//    dataStream.print()
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
//    unionStream.print("union")

    env.execute("transform test job")
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading, String]{
  override def map(value: SensorReading): String = {
    "flink"
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}