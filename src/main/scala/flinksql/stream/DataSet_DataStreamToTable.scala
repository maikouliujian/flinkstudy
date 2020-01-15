package flinksql.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.sinks.CsvTableSink

object DataSet_DataStreamToTable {

  def main(args: Array[String]): Unit = {

    // 1. 获取流式处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 获取Table处理环境
    //val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,bsSettings)

    // 3. 加载本地集合
    val dataStream: DataStream[Order1] = env.fromCollection(List(
      Order1(1, "beer", 3),
      Order1(2, "diaper", 4),
      Order1(3, "rubber", 2)
    ))

    // 4. 转换为表
    tableEnv.registerDataStream("order1",dataStream)

    // 5. 执行SQL
    val table: Table = tableEnv.sqlQuery("select * from order1 where id=2")

    // 6.写入CSV
    // 打印表结构
    table.printSchema()
//    @param path CSV写入的文件路径
//    @param fieldDelim 各个字段的分隔符
//    @param numFiles 写入的文件的个数
//    @param writeMode 是否覆盖文件
   //   table.writeToSink(new CsvTableSink("./data/score_sql.csv",",",1,FileSystem.WriteMode.OVERWRITE))
    // 7.执行任务
    env.execute()

  }

  case class Order1(id:Long,proudct:String,amount:Int)
}
