import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * RichFlatMapFunction[IN, OUT]
  *
  * IN : 输入数据的类型
  * OUT: 输出数据的类型
  *
  */
class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  // 声明状态值
  private var sum: ValueState[(Long, Long)] = _

  // 重写flatmap方法
  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    // access the state value
    // 通过value方法,获取状态值
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    // 如果状态值为null,赋一个默认值(0,0), 否则返回状态值
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }


    // update the count
    // 累加数据  (1L, 3L)  (1,3)  (1L, 5L)  (2,8)
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    // 更新状态值
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    //    (2,8) (1,4)
    // 当数据累加到大于等于2,就会去求平均值,接着清理状态值
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}


object ExampleCountWindowAverage extends App {
  // 加载流处理环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 加载本地集合
  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 4L),
    (1L, 2L)
  )).keyBy(_._1)      // 分组,根据元组的第一个元素
    .flatMap(new CountWindowAverage())  // 进行自定义FlatMap
    .print()          // 打印结果
  // the printed output will be (1,4) and (1,5)

  // 执行任务
  env.execute("ExampleManagedState")
}