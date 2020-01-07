package flinksql.withhive

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * @author lj
  * @createDate 2019/12/30 17:16
  *
  **/
object FlinkWithHive {
  //todo Flink中现时不支持Hive内置内置。要使用Hive内置函数，用户必须首先在Hive Metastore中手动注册它们。
  //
  //todo 仅在Blink planner中测试了Flink 批处理对Hive功能的支持。
  //
  //todo Hive函数当前不能在Flink中的各个 catalog 之间使用。
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table对象
    //val tableEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,bsSettings)

    val name            = "myhive"
    val defaultDatabase = "mydatabase"
    val hiveConfDir     = "/opt/hive-conf"
    val version         = "2.3.4" // or 1.2.1
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)
  }

}
