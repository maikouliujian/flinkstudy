package common

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author lj
  * @createDate 2019/12/27 11:34
  **/
object DateUtils {

  val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def timestampToDate(timestamp:Long) : String = {
    val date = new Date(timestamp)
    format.format(date)
  }

}
