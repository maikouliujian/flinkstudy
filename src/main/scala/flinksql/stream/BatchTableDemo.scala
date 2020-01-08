package flinksql.stream

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

object BatchTableDemo {

  def main(args: Array[String]): Unit = {
    // 1. 获取批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2. 获取Table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // 3. 加载外部CSV文件
    val csvTableSource: CsvTableSource = CsvTableSource.builder()
      .path("./data/score.csv")     // 加载文件路径
      .field("id", Types.INT)   // 列名,类型定义
      .field("name", Types.STRING)
      .field("subjectId", Types.INT)
      .field("score", Types.DOUBLE)
      .fieldDelimiter(",")      // 属性间分隔符
      .lineDelimiter("\n")      // 换行符
//      .ignoreFirstLine()              // 忽略第一行内容
      .ignoreParseErrors()              // 忽略解析错误
      .build()

    // 4. 将外部数据构建成表
    tableEnv.registerTableSource("tableA",csvTableSource)
    // 5. 使用table方式查询数据
    val table: Table = tableEnv.scan("tableA")
      .select("id,name,subjectId,score")
      .filter("name=='张三'")

    // 6. 打印表结构
    table.printSchema()
    // 7. 将数据落地到新的CSV文件中
    table.writeToSink(new CsvTableSink("./data/score_table.csv",",",1,
      FileSystem.WriteMode.OVERWRITE))
    // 8. 执行任务
    env.execute()
  }
}
