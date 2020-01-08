package flinksql.stream

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.types.Row

import scala.util.Random

object StreamFlinkSqlDemo {

  //  4. 创建一个订单样例类`Order`，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id: String, userId: Int, money: Int, createTime: Long)

  def main(args: Array[String]): Unit = {
    //  1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    2. 获取Table运行环境
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //    3. 设置处理时间为`EventTime`
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //  5. 创建一个自定义数据源
    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        //    - 使用for循环生成1000个订单
        for (i <- 0 until 1000) {
          //  - 随机生成订单ID（UUID）
          val id = UUID.randomUUID().toString
          //  - 随机生成用户ID（0-2）
          val userId = Random.nextInt(3)
          //  - 随机生成订单金额（0-100）
          val money = Random.nextInt(101)
          //  - 时间戳为当前系统时间
          val timestamp = System.currentTimeMillis()
          // 收集数据
          ctx.collect(Order(id, userId, money, timestamp))
          //  - 每隔1秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }

      }

      override def cancel(): Unit = {

      }
    })

    //  6. 添加水印，允许延迟2秒
    val waterDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order]() {

      var currentTimeStamp = 0L

      // 获取水印
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - 2000)
      }

      // 获取当前时间
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        currentTimeStamp = Math.max(element.createTime, previousElementTimestamp)
        currentTimeStamp
      }
    })

    //  7. 导入`import org.apache.flink.table.api.scala._`隐式参数
    import org.apache.flink.table.api.scala._

    //    8. 使用`registerDataStream`注册表，并分别指定字段，还要指定rowtime字段
    tableEnv.registerDataStream("t_order",waterDataStream,'id, 'userId, 'money, 'createTime.rowtime)
    //  9. 编写SQL语句统计用户订单总数、最大金额、最小金额
    //  - 分组时要使用`tumble(时间列, interval '窗口时间' second)`来创建窗口
    val sql =
      """
        |select
        | userId,
        | count(1) as totalCount,
        | max(money) as maxMoney,
        | min(money) as minMoney
        | from
        | t_order
        | group by
        | userId,
        | tumble(createTime, interval '5' second)
      """.stripMargin
    //  10. 使用`tableEnv.sqlQuery`执行sql语句
    val table: Table = tableEnv.sqlQuery(sql)
    //    11. 将SQL的执行结果转换成DataStream再打印出来
    tableEnv.toAppendStream[Row](table).print()
    //    12. 启动流处理程序
    env.execute()
  }

}
