package asyncio

import java.sql.DriverManager
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector


class SideFlatMapFunction extends RichFlatMapFunction[AdData, AdData] {

  private var sideInfo: AtomicReference[java.util.Map[Int, Int]] = _

  override def open(parameters: Configuration): Unit = {

    sideInfo = new AtomicReference[java.util.Map[Int, Int]]()
    sideInfo.set(loadData)

    val executors = Executors.newSingleThreadScheduledExecutor()
    executors.scheduleAtFixedRate(new Runnable {

      override def run(): Unit = reload()

    }, 5, 5, TimeUnit.MINUTES)  //第一次延迟五分钟执行一次，后续每五分钟执行一次！！！

  }


  override def flatMap(value: AdData, out: Collector[AdData]): Unit = {
    val tid = value.tId
    val aid = sideInfo.get().get(tid)
    var newV = AdData(aid, value.tId, value.clientId, value.actionType, value.time)
    out.collect(newV)

  }


  def reload() = {

    try {
      println("do reload~")
      val newData = loadData()
      sideInfo.set(newData)
      println("reload ok~")

    } catch {

      case e: Exception => {

        e.printStackTrace()

      }

    }

  }


  def loadData(): util.Map[Int, Int] = {
    val data = new util.HashMap[Int, Int]()
    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection("jdbc:mysql://localhost:3306/paul", "root", "123456")
    val sql = "select aid,tid from ads"
    val statement = con.prepareStatement(sql)
    val rs = statement.executeQuery()
    while (rs.next()) {
      val aid = rs.getInt("aid")
      val tid = rs.getInt("tid")
      data.put(tid, aid)

    }
    con.close()
    data
  }

}