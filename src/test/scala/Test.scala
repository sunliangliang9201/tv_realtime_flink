import java.text.SimpleDateFormat
import java.util.Date
import java.util.BitSet

import com.bftv.dt.realtime.utils.MyBloomFilter

/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    println(new MyBloomFilter().bitSetSize)
  }
}
