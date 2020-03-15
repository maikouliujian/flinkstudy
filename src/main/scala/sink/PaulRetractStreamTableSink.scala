package sink

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row

class PaulRetractStreamTableSink extends RetractStreamTableSink[Row] {

  private var fieldNames: Array[String] = _
  private var fieldTypes: Array[TypeInformation[_]] = _


  /**
    *
    * 该tableSink的schema
    *
    * @return
    *
    */

  override def getRecordType: TypeInformation[Row] = {
    new RowTypeInfo(fieldTypes, fieldNames)
  }


  /**
    *
    * 字段名称
    *
    * @return
    *
    */

  override def getFieldNames: Array[String] = fieldNames


  /**
    *
    * 字段类型
    *
    * @return
    *
    */

  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes


  /**
    *
    * 内部调用，会自动将上游输出类型、字段配置进来
    *
    * @param fieldNames
    * @param fieldTypes
    * @return
    *
    */

  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[org.apache.flink.api.java.tuple.Tuple2[java.lang.Boolean, Row]] = {
    this.fieldNames = fieldNames
    this.fieldTypes = fieldTypes
    this
  }

  /**
    *
    * @param dataStream 上游的table 在内部会换为dataStream
    *
    *                   Boolean: 表示消息编码，insert 会编码为true,只会产生一条数据
    *
    *                   update 会编码为false/true, 会产生两条数据，
    *
    *                   第一条false表示需要撤回，也就是删除的数据
    *
    *                   第二条true表示需要插入的数据
    *
    *                   Row : 表示真正的数据
    *
    */
  override def emitDataStream(dataStream: datastream.DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
    dataStream.print()
  }
}
