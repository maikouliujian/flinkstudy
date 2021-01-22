package flinksql.stream

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable
/**
  * @author lj
  * @createDate 2019/12/26 14:18
  **/
object WindowJoinITCase {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table对象
    val tEnv = StreamTableEnvironment.create(env)
    //val tEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！

    val sqlQuery =
      """
        |SELECT t1.key, HOP_END(t1.rowtime, INTERVAL '4' SECOND, INTERVAL '20' SECOND), COUNT(t1.key)
        |FROM T1 AS t1
        |GROUP BY HOP(t1.rowtime, INTERVAL '4' SECOND, INTERVAL '20' SECOND), t1.key
        |""".stripMargin

    val data1 = new mutable.MutableList[(String, String, Long)]
    data1.+=(("A", "L-1", 1000L))
    data1.+=(("A", "L-2", 2000L))
    data1.+=(("A", "L-3", 3000L))
    //data1.+=(("B", "L-8", 2000L))
    data1.+=(("B", "L-4", 4000L))
    data1.+=(("C", "L-5", 2100L))
    data1.+=(("A", "L-6", 10000L))
    data1.+=(("A", "L-7", 13000L))
    import scala.collection.JavaConversions._
//    val t1 = env.fromCollection(data1)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(1)){
//        override def extractTimestamp(element: (String, String, Long)): Long = {
//          element._3 * 1000
//        }
//      })
//      .toTable(tEnv, 'key, 'id, 'rowtime)
//
//    tEnv.registerTable("T1", t1)
//
//    val t_r = tEnv.sqlQuery(sqlQuery)
//    val result = t_r.toAppendStream[Row]
//
//    result.print("sql_test")
//    result.addSink(sink)
    env.execute()
  }


}

