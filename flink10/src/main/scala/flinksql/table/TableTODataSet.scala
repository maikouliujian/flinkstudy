package flinksql.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment

object TableTODataSet {

  def main(args: Array[String]): Unit = {

    //    1. 获取批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    2. 设置并行度
    env.setParallelism(1)
    //    3. 获取Table运行环境
    //val tableEnv = TableEnvironment.getTableEnvironment(env)
    val tableEnv = BatchTableEnvironment.create(env)
    //    4. 加载本地集合
    val dataSet = env.fromCollection(List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (8L, 8, "Hello World"),
      (20L, 20, "Hello World")
    )
    )
    //    5. 转换DataSet为Table
    val table: Table = tableEnv.fromDataSet(dataSet)
    // 6. Table转换为DataSet
    val result: DataSet[(Long, Int, String)] = tableEnv.toDataSet[(Long, Int, String)](table)
    //    7. 打印输出
    result.print()
    //    8. 执行任务
//    env.execute()
  }
}
