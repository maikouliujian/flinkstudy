package flinksql.batch

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * @author lj
  * @createDate 2019/12/26 12:02
  **/

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

object SqlAPI {

  def main(args: Array[String]): Unit = {

//    val env = ExecutionEnvironment.getExecutionEnvironment
//    // 创建table对象
//    //val tableEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！
//    val tableEnv = BatchTableEnvironment.create(env)
//
//    //Stream 或者 dataSet 与Table的转换
//    val dataStream: DataStream[(Int, Int)] = env.fromElements((1, 2), (12, 23), (15, 22))
//
//    // 转换DataStream 为带有字段名的"myLong", "myString" (基于位置)Table
//    //val table: Table = tableEnv.fromDataStream(dataStream, 'myLong, 'myString)
//
//    //todo 写法1
//    tableEnv.registerDataStream("table1", dataStream, 'myLong, 'myString)
//    val table: Table = tableEnv.sqlQuery("select myLong,myString from table1")
//    //todo 写法2
//    /*val table_demo1: Table = tableEnv.fromDataStream(dataStream, 'myLong, 'myString)
//    val table: Table = tableEnv.sqlQuery(s"select myLong,myString from $table_demo1")*/
//
//    //基于proctime创建滚动窗口，并制定10秒切为一个窗口,
//    //val table: Table = tableEnv.sqlQuery("select id,sum(type) from table1 group by tumble(proctime, interval '10' SECOND),id")
//
//    ////基于rowtime【event time】创建滚动窗口，并制定5秒切为一个窗口
//    //val table: Table = tableEnv.sqlQuery("select id,sum(type) from table1 group by tumble(rowtime, interval '5' SECOND),id")
//
//
//
//    //打印输出
//    //TODO toAppendStream 只对增加的新数据有效,较为局限
//    //TODO  toRetractStream 适用于各种类型====>更新的数据为true,未更新为false
//    val rs: DataStream[(Integer, Integer)] = tableEnv.toAppendStream[(Integer, Integer)](table)
//    //val  rs1: DataStream[(Boolean,(Integer, Integer))] = tableEnv.toRetractStream[(Integer, Integer)](table)
//
//    rs.print()
//    //rs1.print()
//    env.execute()
  }

}
