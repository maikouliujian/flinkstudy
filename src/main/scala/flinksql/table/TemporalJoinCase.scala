package flinksql.table

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema, Types}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.types.Row

/**
  * @author lj
  * @createDate 2019/12/26 14:18
  **/
object TemporalJoinCase {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table对象
    val tEnv = StreamTableEnvironment.create(env)
    //val tEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！


    //方便我们查出输出数据
    env.setParallelism(1)

    val sourceTableName = "RatesHistory"
    // 创建CSV source数据结构
    val tableSource = CsvTableSourceUtils.genRatesHistorySource
    // 注册source
    tEnv.registerTableSource(sourceTableName, tableSource)

    // 注册retract sink
    val sinkTableName = "retractSink"
    val fieldNames = Array("rowtime", "currency", "rate")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.STRING)

//    tEnv.registerTableSink(
//      sinkTableName,
//      new MySink(fieldNames,fieldTypes))

    val SQL =
      """
        |SELECT *
        |FROM RatesHistory AS r
        |WHERE r.rowtime = (
        |  SELECT MAX(rowtime)
        |  FROM RatesHistory AS r2
        |  WHERE r2.currency = r.currency
        |  AND r2.rowtime <= '10:58:00'  )
      """.stripMargin

    // 执行查询
    tEnv.sqlQuery(SQL)
    env.execute()

  }


}

//class MySink(fieldNames:Array[String],fieldTypes:Array[TypeInformation[_]])
//  extends UpsertStreamTableSink[Row] {
//
////  private var tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes)
//
//  override def consumeDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]):
//             DataStreamSink[tuple.Tuple2[lang.Boolean, Row]] = {
//       //dataStream.addSink(new RichSinkFunction())
//       null
//  }
//
//  override def setKeyFields(keys: Array[String]): Unit = {}
//
//  override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {}
//
//  override def getRecordType: TypeInformation[Row] = {
//    new RowTypeInfo(fieldTypes,fieldNames)
//  }
//
//  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
//
//  }
//
//  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]):{
//
//  }
//
//
//
//  class MySinkFunction() extends RichSinkFunction[Tuple2[lang.Boolean, Row]] {
//    override def invoke(value: (lang.Boolean, Row), context: SinkFunction.Context[_]): Unit = {
//      val flag = value._1
//      if(flag){
//        println("增加... "+value)
//      }else {
//        println("删除... "+value)
//      }
//    }
//  }
//}



