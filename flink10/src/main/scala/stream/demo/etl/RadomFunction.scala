package stream.demo.etl

import org.apache.flink.streaming.api.functions.source.SourceFunction
import util.StringUtil

/**
  * @author lj
  * @createDate 2019/12/27 17:55
  **/
class RadomFunction extends SourceFunction[String]{
  var flag = true
  override def cancel(): Unit = {
    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (flag){
      for (i <- 0 to 300) {
        var nu = i.toString
        while (nu.length < 3) {
          nu = "0" + nu
        }
        ctx.collect(nu + "," + StringUtil.getRandomString(5))
        Thread.sleep(2000)
      }
    }
  }
}
