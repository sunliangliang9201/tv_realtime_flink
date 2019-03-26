import java.text.SimpleDateFormat
import java.util.Date

/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val a = format.format(new Date(System.currentTimeMillis()))
    println(a)
  }
}
