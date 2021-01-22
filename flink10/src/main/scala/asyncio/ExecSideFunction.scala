package asyncio

import java.util.concurrent.{ArrayBlockingQueue, Executor, ThreadPoolExecutor, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.datanucleus.store.rdbms.datasource.dbcp.ConnectionFactory

class ExecSideFunction extends RichAsyncFunction[String, String] {


  var executors: Executor = _
  var sqlTemplate: String = _


  override def open(parameters: Configuration): Unit = {
    executors = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](1000))
    sqlTemplate = "select value from tbl1 where id=?"

  }


  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executors.execute(new Runnable {
      override def run(): Unit = {
//        val con = ConnectionFactory.getConnection("sourceId").asInstanceOf[Connection]
//        val sql = sqlTemplate.replace("?", parseKey(input))
//        MysqlUtil.executeSelect(con, sql, rs => {
//          val res = new util.ArrayList[String]()
//          while (rs.next()) {
//            val v = rs.getString("value")
//            res.add(fillData(input, v))
//          }
//          resultFuture.complete(res)
//        })
//        con.close()

      }

    })

  }


  private def parseKey(input: String): String = {
    ""

  }


  private def fillData(input: String, v: String): String = {
    ""

  }

}
