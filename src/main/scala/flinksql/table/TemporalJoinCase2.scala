package flinksql.table

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

import scala.collection.mutable
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row


/**
  * @author lj
  * @createDate 2019/12/26 14:18
  *
  *            todo 看 https://blog.csdn.net/u013411339/article/details/88840356
  **/
object TemporalJoinCase2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table对象
    //val tEnv = StreamTableEnvironment.create(env)
    //val tEnv = TableEnvironment.getTableEnvironment(env) //TODO 已经过期了！！！

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env,bsSettings)


    //方便我们查出输出数据
    env.setParallelism(1)

    // 设置时间类型是 event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(Long, String, Timestamp)]
    ordersData.+=((2L, "Euro", new Timestamp(2L)))
    ordersData.+=((1L, "US Dollar", new Timestamp(3L)))
    ordersData.+=((50L, "Yen", new Timestamp(4L)))
    ordersData.+=((3L, "Euro", new Timestamp(7L)))

    //构造汇率数据
    val ratesHistoryData = new mutable.MutableList[(String, Long, Timestamp)]
    ratesHistoryData.+=(("US Dollar", 102L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 114L, new Timestamp(1L)))
    ratesHistoryData.+=(("Yen", 1L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 116L, new Timestamp(5L)))
    ratesHistoryData.+=(("Euro", 119L, new Timestamp(7L)))

    import scala.collection.JavaConversions._
    // 进行订单表 event-time 的提取
    val orders = env.fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new OrderTimestampExtractor[Long, String]())
    //todo 下面两种方式都可以使用！！！！
      //.toTable(tEnv, 'amount, 'currency, 'rowtime.rowtime)
    // 注册订单表和汇率表
    //tEnv.registerTable("Orders", orders)
    import org.apache.flink.table.api.scala._
    tEnv.registerDataStream("Orders",orders,'amount, 'currency, 'rowtime.rowtime)

    // 进行汇率表 event-time 的提取
    val ratesHistory = env
      .fromCollection(ratesHistoryData)
      .assignTimestampsAndWatermarks(new OrderTimestampExtractor[String, Long]())
      .toTable(tEnv, 'currency, 'rate, 'rowtime.rowtime)
    tEnv.registerTable("RatesHistory", ratesHistory)

    //=================================创建  TemporalTable 的逻辑  ======================================//
    val tab = tEnv.scan("RatesHistory")

    // 创建TemporalTableFunction
    //todo 时间属性，相当于版本信息
    /***
      *
      * @param timeAttribute Must points to a time indicator. Provides a way to compare which
      *                      records are a newer or older version.
      * @param primaryKey    Defines the primary key. With primary key it is possible to update
      *                      a row or to delete it.
      * @return { @link TemporalTableFunction} which is an instance of { @link TableFunction}.
      *         It takes one single argument, the { @code timeAttribute}, for which it returns
      *         matching version of the { @link Table}, from which { @link TemporalTableFunction}
      *         was created.
      */
    val temporalTableFunction = tab.createTemporalTableFunction('rowtime, 'currency)
    //注册TemporalTableFunction
    tEnv.registerFunction("Rates",temporalTableFunction)

    val SQLQuery =
      """
        |SELECT o.currency, o.amount, r.rate,
        |  o.amount * r.rate AS yen_amount
        |FROM
        |  Orders AS o,
        |  LATERAL TABLE (Rates(o.rowtime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin
    // TODO ===> LATERAL TABLE (Rates(o.rowtime)) AS r ：可以这么理解，通过Orders.rowtime 可以找到对应时间点【相差最小】的
    // TODO RatesHistory表的全部信息！！！

    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(SQLQuery))

    val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
    // 打印查询结果
    result.print()
    env.execute()

  }
}


class OrderTimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}



