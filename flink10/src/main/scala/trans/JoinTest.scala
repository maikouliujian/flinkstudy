package trans

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * @author lj
  * @createDate 2020/1/20 17:51
  **/

case class Person1(id:Int,name:String)
object JoinTest {

  def main(args: Array[String]): Unit = {

    // env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataset1 = env.fromElements(Person1(1,"peter"),Person1(2,"Alice"))
    val dataset2 = env.fromElements((12.3,1),(22.3,3))

    //1、
    //val value: JoinDataSet[Person, (Double, Int)] = dataset1.join(dataset2).where("id").equalTo(1)
     //2、
//    val result: DataSet[(Int, String, Double)] = dataset1.join(dataset2).where("id").equalTo(1) {
//      (left, right) => (left.id, left.name, right._1 + 1)
//    }
   //3、
//    val result: DataSet[(Int, String, Double)] = dataset1.join(dataset2).where("id").equalTo(1) {
//      (left, right, collector:Collector[(String,Double)]) => {
//        collector.collect(left.name,right._1 +1)
//        collector.collect("prefix_" + left.name,right._1 +2)
//      }
//    }

    //4、第二个是小数据集
    //val result = dataset1.joinWithTiny(dataset2).where("id").equalTo(1)
    //5、第一个是小数据集
    //val result = dataset1.joinWithHuge(dataset2).where("id").equalTo(1)

    //6、不同的join参数
    /****
      *
      * @Public
      * public static enum JoinHint {
      *
      * /**
      * * Leave the choice how to do the join to the optimizer. If in doubt, the
      * * optimizer will choose a repartitioning join.
      **/
      * OPTIMIZER_CHOOSES,
      *
      * /**
      * * Hint that the first join input is much smaller than the second. This results in
      * * broadcasting and hashing the first input, unless the optimizer infers that
      * * prior existing partitioning is available that is even cheaper to exploit.
      * */
      * BROADCAST_HASH_FIRST,
      *
      * /**
      * * Hint that the second join input is much smaller than the first. This results in
      * * broadcasting and hashing the second input, unless the optimizer infers that
      * * prior existing partitioning is available that is even cheaper to exploit.
      * */
      * BROADCAST_HASH_SECOND,
      *
      * /**
      * * Hint that the first join input is a bit smaller than the second. This results in
      * * repartitioning both inputs and hashing the first input, unless the optimizer infers that
      * * prior existing partitioning and orders are available that are even cheaper to exploit.
      * */
      * REPARTITION_HASH_FIRST,
      *
      * /**
      * * Hint that the second join input is a bit smaller than the first. This results in
      * * repartitioning both inputs and hashing the second input, unless the optimizer infers that
      * * prior existing partitioning and orders are available that are even cheaper to exploit.
      * */
      * REPARTITION_HASH_SECOND,
      *
      * /**
      * * Hint that the join should repartitioning both inputs and use sorting and merging
      * * as the join strategy.
      * */
      * REPARTITION_SORT_MERGE
      */
    //val result = dataset1.join(dataset2,JoinHint.BROADCAST_HASH_FIRST).where("id").equalTo(1)

    //7、
    //val value: CoGroupDataSet[Person, (Double, Int)] = dataset1.coGroup(dataset2).where("id").equalTo(1)

    //8、
    val value: CrossDataSet[Person1, (Double, Int)] = dataset1.cross(dataset2)

    value.print()


  }

}
