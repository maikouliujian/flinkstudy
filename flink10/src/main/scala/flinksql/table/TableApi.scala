package flinksql.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Avro, Kafka, Rowtime, Schema}
import org.apache.flink.table.types.{DataType, FieldsDataType}

/**table api 连接外部系统
  * @author lj
  * @createDate 2019/12/30 16:57
  **/

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.DataTypes._
object TableApi {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table对象
    //val tableEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,bsSettings)


    //设置ttl
//    import org.apache.flink.api.common.state.StateTtlConfig
//    import org.apache.flink.api.common.state.ValueStateDescriptor
//    val ttlConfig: StateTtlConfig = StateTtlConfig
//      .newBuilder(org.apache.flink.api.common.time.Time.seconds(1))
//      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build
//
//    val stateDescriptor = new ValueStateDescriptor[String]("text state", classOf[String])
//    stateDescriptor.enableTimeToLive(ttlConfig)
    //旧的

//    val config=tableEnv.queryConfig.withIdleStateRetentionTime(Time.minutes(1),Time.minutes(6))
//    tabEnv.sqlUpdate('"',config)
//    tabEnv.sqlQuery("",config)
//    tab.writeToSink(sink,config)
    //新的
    val config = tableEnv.getConfig.setIdleStateRetentionTime(org.apache.flink.api.common.time.Time.minutes(1),
                                                              org.apache.flink.api.common.time.Time.minutes(6))

    // access flink configuration
    val configuration = tableEnv.getConfig().getConfiguration()
    // set low-level key-value options
    configuration.setString("table.exec.mini-batch.enabled", "true") // enable mini-batch optimization
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s") // use 5 seconds to buffer input records
    configuration.setString("table.exec.mini-batch.size", "5000") // the maximum number of records can be buffered by each aggregate operator task

    tableEnv
      // declare the external system to connect to
      .connect(
      new Kafka()
        .version("0.10")
        .topic("test-input")
        .startFromEarliest()
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )

      // declare a format for this system
      .withFormat(
      new Avro()
        .avroSchema(
          "{" +
            "  \"namespace\": \"org.myorganization\"," +
            "  \"type\": \"record\"," +
            "  \"name\": \"UserMessage\"," +
            "    \"fields\": [" +
            "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
            "      {\"name\": \"user\", \"type\": \"long\"}," +
            "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
            "    ]" +
            "}"
        )
    )

      // declare the schema of the table
      //import org.apache.flink.table.api.DataTypes._
      .withSchema(
      new Schema()
        //TODO 过期
        //.field("rowtime", Types.SQL_TIMESTAMP)
        .field("rowtime",DataTypes.TIMESTAMP())
        .rowtime(new Rowtime()
          .timestampsFromField("timestamp")
          .watermarksPeriodicBounded(60000)
        )
        //.field("user", Types.LONG)
        .field("user", DataTypes.FLOAT())
        //.field("message", Types.STRING)
        .field("message", DataTypes.STRING)
    )

      // specify the update-mode for streaming tables
      .inAppendMode()
      // register as source, sink, or both and under a name
      //TODO 过期
      //.registerTableSource("MyUserTable")
        .createTemporaryTable("MyUserTable")
  }

}
