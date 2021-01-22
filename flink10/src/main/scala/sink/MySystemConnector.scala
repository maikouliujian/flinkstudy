package sink

import java.util

import org.apache.flink.table.descriptors.ConnectorDescriptor

/**
  * Connector to MySystem with debug mode.
  */
class MySystemConnector(isDebug: Boolean) extends ConnectorDescriptor("my-system", 1, false) {

  override protected def toConnectorProperties(): util.Map[String, String] = {
    val properties = new util.HashMap[String, String]
    properties.put("connector.debug", isDebug.toString)
    properties
  }
}
