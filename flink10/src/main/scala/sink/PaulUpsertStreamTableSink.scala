package sink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.types.Row
import java.lang

import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream


/***
  * 在这个例子中unique key 就表示word 字段，会调用setKeyFields自动设置。
  * setIsAppendOnly 表示是否AppendOnly模式，在这个例子中包含更新模式，所以为false,
  * 对于仅仅是单条插入或者窗口函数聚合类的表示的是AppendOnly，为true。
  * 需要注意的是unique key的存在与AppendOnly为true并没有必然关系，窗口函数聚合类的AppendOnly为true,同时存在unique key，
  * 单条输出(例如select word from table1)类的AppendOnly为true，
  * 但是unique key不存在。相反如果AppendOnly为false ，那么unique key则必然存在
  */
class PaulUpsertStreamTableSink extends UpsertStreamTableSink[Row] {


  private var fieldNames:Array[String]=_

  private var fieldTypes:Array[TypeInformation[_]]=_


  private var keys: Array[String]=_

  private var isAppendOnly:lang.Boolean=_


  /**

    * unique key

    * @param keys

    */

  override def setKeyFields(keys: Array[String]): Unit = {

    this.keys=keys

  }


  override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {

    this.isAppendOnly=isAppendOnly

  }


  override def getRecordType: TypeInformation[Row] = {

    new RowTypeInfo(fieldTypes,fieldNames)

  }


  override def getFieldNames: Array[String] = fieldNames


  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes


  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {


    this.fieldNames=fieldNames

    this.fieldTypes=fieldTypes

    this


  }

  override def emitDataStream(dataStream: datastream.DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
    dataStream.print()
  }
}
