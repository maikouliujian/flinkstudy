package sink

import org.apache.flink.table.factories.StreamTableSourceFactory
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import java.util
class MySystemTableSourceFactory extends StreamTableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put("update-mode", "append")
    context.put("connector.type", "my-system")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("connector.debug")
    properties
  }

  override def createStreamTableSource(properties: util.Map[String, String]): StreamTableSource[Row] = {
    val isDebug = java.lang.Boolean.valueOf(properties.get("connector.debug"))
    //additional validation of the passed properties can also happen here
    null
    //todo 自定义source
    //new MySystemAppendTableSource(isDebug)
  }
}
