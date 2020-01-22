package trans

import org.apache.flink.api.scala._

/**
  * @author lj
  * @createDate 2020/1/20 18:31
  **/
object Iteration {
  def main(args: Array[String]): Unit = {
    // env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //TODO flink的迭代计算分为两种：Bulk Iteration(全量迭代计算) 和Delt Iteration（增量迭代计算）


    //todo:1、 Bulk Iteration(全量迭代计算)
//    val initial = env.fromElements(0)
//    val count = initial.iterate(10000) { iterationInput: DataSet[Int] => {
//      val result = iterationInput.map(i => {
//        val x = Math.random()
//        val y = Math.random()
//        i + (if (x * x + y * y < 1) 1 else 0)
//      }
//      )
//      result
//    }
//
//    }
//
//    val result = count map {c => c / 10000.0 * 4}
//    result.print()
    //env.execute("pi")


    //todo:2、elt Iteration（增量迭代计算）



  }

}
