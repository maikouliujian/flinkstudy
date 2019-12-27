package study

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.calcite.avatica.proto.Common
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

/**  * @author lj
  * @createDate 2019/12/27 10:30
  **/
object KafkaCurrentDayMaker {

  var minute : Int = 1
  val calendar: Calendar = Calendar.getInstance()

  /**
    * 一天时间比较长，不方便观察，将时间改为当前时间，
    * 每次累加10分钟，这样一天只需要144次循环，也就是144秒
    * @return
    */
  def getCreateTime(): String = {
    //    minute = minute + 1
    calendar.add(Calendar.MINUTE, 10)
    sdf.format(calendar.getTime)
  }
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.1.163:9092,192.168.1.164:9092,192.168.1.165:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
    // 初始化开始时间为当前时间
    calendar.setTime(new Date())
    println(sdf.format(calendar.getTime))
    var i =0
    while (true) {

      //      val map = Map("id"-> i, "createTime"-> sdf.format(System.currentTimeMillis()))
      val map = Map("id"-> i, "createTime"-> getCreateTime())
      val jsonObject: JSONObject = new JSONObject(map)
      println(jsonObject.toString())
      // topic current_day
      val msg = new ProducerRecord[String, String]("bigdata", jsonObject.toString())
      producer.send(msg)
      producer.flush()
      // 控制数据频率
      Thread.sleep(50)
      i = i + 1
    }
  }

}
