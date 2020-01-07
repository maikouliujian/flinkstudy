package stream.demo.etl

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import common.Common
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import util.TwoStringSource
import java.util
import java.util.{Timer, TimerTask}

import org.slf4j.{Logger, LoggerFactory}

/**
  * @author lj
  * @createDate 2019/12/27 17:55
  **/

import org.apache.flink.api.scala._

object FlinkTimerDemo {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger("BroadCastDemo")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 自定义的source，输出 x,xxx 格式随机字符
    val input = env.addSource(new TwoStringSource)
    val stream = input.map(new RichMapFunction[String, String] {

      val jdbcUrl = "jdbc:mysql://venn:3306?useSSL=false&allowPublicKeyRetrieval=true"
      val username = "root"
      val password = "123456"
      val driverName = "com.mysql.jdbc.Driver"
      var conn: Connection = null
      var ps: PreparedStatement = null
      val map = new util.HashMap[String, String]()

      override def open(parameters: Configuration): Unit = {
        logger.info("init....")
        query()
        // new Timer
        val timer = new Timer(true)
        // schedule is 10 second, 5 second between successive task executions 定义了一个10秒的定时器，
        // 定时执行查询数据库的方法
        timer.schedule(new TimerTask {
          override def run(): Unit = {
            query()
          }
        }, 10000)

      }

      override def map(value: String): String = {
        // concat input and mysql data，简单关联输出
        value + "-" + map.get(value.split(",")(0))
      }

      /**
        * query mysql for get new config data
        */
      def query() = {
        logger.info("query mysql")
        try {
          Class.forName(driverName)
          conn = DriverManager.getConnection(jdbcUrl, username, password)
          ps = conn.prepareStatement("select id,name from venn.timer")
          val rs = ps.executeQuery

          while (!rs.isClosed && rs.next) {
            val id = rs.getString(1)
            val name = rs.getString(2)
            // 将结果放到 map 中
            map.put(id, name)
          }
          logger.info("get config from db size : {}", map.size())

        } catch {
          case e@(_: ClassNotFoundException | _: SQLException) =>
            e.printStackTrace()
        } finally {
          ps.close()
          conn.close()
        }
      }
    })
    //          .print()


//    val sink = new FlinkKafkaProducer[String]("timer_out"
//      , new MyKafkaSerializationSchema[String]()
//      , Common.getProp
//      , FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
//    stream.addSink(sink)
//
//    env.execute("BroadCastDemo")
  }

}
