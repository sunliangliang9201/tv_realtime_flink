/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    val a = "insert into bftv.tv_display_window_active(end_window,counts) values(?,?)"
    println(a)
    println(a.stripMargin)
  }
}