package sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig

object Demo1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO 过期
    //val tabEnv = StreamTableEnvironment.getTableEnvironment(env)

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv = StreamTableEnvironment.create(env,bsSettings)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "gid1")

    import org.apache.flink.api.scala._

    val consumer = new FlinkKafkaConsumer011[String]("topic1",
      new SimpleStringSchema, kafkaConfig)

    val ds = env.addSource(consumer)
      .map((_, 1))

    import org.apache.flink.table.api.scala._
    //TODO 过期
    //tabEnv.registerDataStream("table1", ds, 'word, 'cnt)
    tabEnv.createTemporaryView("table1", ds, 'word, 'cnt)

    val rsTable = tabEnv.sqlQuery("select word,sum(cnt) from table1 group by word")

    val tableSink = new PaulRetractStreamTableSink
    //TODO api不存在
    //rsTable.writeToSink(tableSink)
    //TODO 过期  * @deprecated Use {@link #connect(ConnectorDescriptor)} instead.
    tabEnv.registerTableSink("sink",tableSink)

    env.execute()

  }

}
