package stream.demo.etl

import java.io.File

import common.Common
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

/**
  * @author lj
  * @createDate 2019/12/27 17:55
  **/

import org.apache.flink.api.scala._
object BroadCastDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if ("/".equals(File.separator)) {
      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
      env.setStateBackend(backend)
      env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)
    } else {
      env.setMaxParallelism(1)
      env.setParallelism(1)
    }
    // 配置更新流
    val configSource = new FlinkKafkaConsumer[String]("broad_cast_demo", new SimpleStringSchema, Common.getProp)
    // 配置流的初始化，可以通过读取配置文件实现
    var initFilePath = ""
    if ("/".equals(File.separator)){
      initFilePath = "hdfs:///venn/init_file.txt"
    }else{
      initFilePath = "D:\\idea_out\\broad_cast.txt"
    }
    val init = env.readTextFile(initFilePath)
    val descriptor = new MapStateDescriptor[String,  String]("dynamicConfig",
      BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    //将configSource 和 init进行合并，作为一个整体，并广播出去！！！
    val configStream = env.addSource(configSource).union(init).broadcast(descriptor)


    val input = env.addSource(new RadomFunction)
      .connect(configStream)
      .process(new BroadcastProcessFunction[String, String, String] {
        override def processBroadcastElement(value: String,
                                             ctx: BroadcastProcessFunction[String, String, String]#Context,
                                             out: Collector[String]): Unit = {

          println("new config : " + value)
          //TODO 广播数据整合逻辑！！！
          val configMap = ctx.getBroadcastState(descriptor)
          // process update configMap，读取配置数据，写入广播状态中
          val line = value.split(",")
          configMap.put(line(0), line(1))
        }
        override def processElement(value: String,
                                    ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                    out: Collector[String]): Unit = {
          // use give key, return value
          val configMap = ctx.getBroadcastState(descriptor)
          // 解析三位城市编码，根据广播状态对应的map，转码为城市对应中文
          //          println(value)
          val line = value.split(",")
          val code = line(0)
          var va = configMap.get(code)
          // 不能转码的数据默认输出 中国(code=xxx)
          if ( va == null){
            va = "中国(code="+code+")"
          }else{
            va = va + "(code="+code+")"
          }
          out.collect(va + "," + line(1))
        }
      })
    input.print()

    env.execute("BroadCastDemo")
  }

}
