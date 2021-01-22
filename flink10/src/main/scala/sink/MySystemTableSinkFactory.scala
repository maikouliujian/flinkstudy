package sink

import java.util

import org.apache.flink.table.factories.{StreamTableSinkFactory, StreamTableSourceFactory}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.types.Row

class MySystemTableSinkFactory extends StreamTableSinkFactory[Row]{
  override def createStreamTableSink(properties: util.Map[String, String]): StreamTableSink[Row] = {
    val isDebug = java.lang.Boolean.valueOf(properties.get("connector.debug"))
    //additional validation of the passed properties can also happen here
    //todo 自定义source
    null
    //new PaulRetractStreamTableSink
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("connector.debug")
    properties
  }

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put("update-mode", "append")
    context.put("connector.type", "my-system")
    context
  }
}
