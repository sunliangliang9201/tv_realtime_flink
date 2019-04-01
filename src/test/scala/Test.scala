import java.text.SimpleDateFormat
import java.util.Date

import java.util.BitSet


/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    val bitset1 = new BitSet(10)
    val bitset2 = new BitSet(8)
    bitset1.set(1, true)
    bitset1.andNot(bitset2)
    println(bitset1.get(2))
  }
}
