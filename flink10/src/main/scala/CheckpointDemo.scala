import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//**开发自定义数据源**

// 1. 自定义样例类(id: Long, name: String, info: String, count: Int)
case class Msg(id: Long, name: String, info: String, count: Int)

// 2. 自定义数据源,继承RichSourceFunction
class MySourceFunction extends RichSourceFunction[Msg] {
  var isRunning = true

  // 3. 实现run方法, 每秒钟向流中注入10000个样例类
  override def run(ctx: SourceFunction.SourceContext[Msg]): Unit = {

    while (isRunning) {
      for (i <- 0 until 10000) {
        // 收集数据
        ctx.collect(Msg(1L, "name_" + i, "test_info", 1))
      }
      // 休眠1s
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

//**开发自定义状态**

//1. 继承Serializable  ListCheckpointed
class UDFState extends Serializable {
  private var count = 0L

  //2. 为总数count提供set和get方法
  def setState(s: Long) = count = s

  def getState = count

}


// **开发自定义Window和检查点**
// 1. 继承WindowFunction
// 3. 继承ListCheckpointed
class MyWindowAndCheckpoint extends WindowFunction[Msg,Long,Tuple,TimeWindow] with ListCheckpointed[UDFState]{

  // 求和总数
  var total = 0L

  // 2. 重写apply方法,对窗口数据进行总数累加
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Msg], out: Collector[Long]): Unit = {

    var count = 0L
    for(msg<-input){
      count = count+1
    }
    total = total + count
    // 收集数据
    out.collect(count)
  }

  // 自定义快照
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[UDFState] = {

    val udfList = new util.ArrayList[UDFState]()
    // 创建UDFState对象
    var udfState = new UDFState
    udfState.setState(total)
    udfList.add(udfState)
    // 返回数据
    udfList
  }

  // 恢复快照
  override def restoreState(state: util.List[UDFState]): Unit = {

    val udfState: UDFState = state.get(0)

    // 取出检查点的值 赋值给total即可
    total = udfState.getState

  }
}



// 4. 重写snapshotState,制作自定义快照
// 5. 重写restoreState,恢复自定义快照

object CheckpointDemo {

  def main(args: Array[String]): Unit = {

    //1. 流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 开启checkpoint,间隔时间为6s
    env.enableCheckpointing(6000)
    //3. 设置checkpoint位置
    env.setStateBackend(new FsStateBackend("file:///E:/dev_checkpoint"))
    //4. 设置处理时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //5. 添加数据源
    import org.apache.flink.api.scala._
    val soureDataStream: DataStream[Msg] = env.addSource(new MySourceFunction)
    //6. 添加水印支持
    val watermarkDataStream: DataStream[Msg] = soureDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Msg]() {

      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      // 抽取当前时间
      override def extractTimestamp(element: Msg, previousElementTimestamp: Long): Long = {
        System.currentTimeMillis()
      }
    })

    //7. keyby分组
    val keyedStream: KeyedStream[Msg, Tuple] = watermarkDataStream.keyBy(0)
    //8. 设置滑动窗口,窗口时间为4s, 滑动时间为1s
    val windowedStream: WindowedStream[Msg, Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(4),Time.seconds(1))
    //9. 指定自定义窗口
    val result: DataStream[Long] = windowedStream.apply(new MyWindowAndCheckpoint)
    //10. 打印结果
    result.print()
    //11. 执行任务
    env.execute()



  }

}
