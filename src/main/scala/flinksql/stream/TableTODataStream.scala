package flinksql.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._

object TableTODataStream {

  def main(args: Array[String]): Unit = {

    //    1. 获取流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    2. 设置并行度
    env.setParallelism(1)
    //    3. 获取Table运行环境
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    4. 加载本地集合
    val dataStream: DataStream[(Long, Int, String)] = env.fromCollection(List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (8L, 8, "Hello World"),
      (20L, 20, "Hello World")
    )
    )
    //    5. 转换DataStream为Table
    val table: Table = tableEnv.fromDataStream(dataStream)
    //    6. 将table转换为DataStream----将一个表附加到流上Append Mode
    val appendDataSteam: DataStream[(Long, Int, String)] = tableEnv.toAppendStream[(Long,Int,String)](table)
    //    7. 将table转换为DataStream----Retract Mode true代表添加消息，false代表撤销消息
    val retractDataStream: DataStream[(Boolean, (Long, Int, String))] = tableEnv.toRetractStream[(Long, Int, String)](table)
    //    8. 打印输出
    appendDataSteam.print()
    //    9. 执行任务
    env.execute()
  }
}
