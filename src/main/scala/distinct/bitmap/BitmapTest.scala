package distinct.bitmap

object BitmapTest {

  def main(args: Array[String]): Unit = {
    val bitSet:java.util.BitSet=new java.util.BitSet()
    bitSet.set(0)
    bitSet.set(1)
    bitSet.get(1) //true

    bitSet.clear(1) //删除
    bitSet.get(1) //false
    bitSet.cardinality()//2
    bitSet.size()

    //接下来存储一个10000的数字：
    bitSet.set(10000)
    bitSet.cardinality()//2
    bitSet.size() //1.22kb
  }

}
