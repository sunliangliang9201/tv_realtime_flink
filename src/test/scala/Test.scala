

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable

/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    val map = mutable.Map[Int, Int](1 -> 1, 3 -> 3, 2 -> 2)
    val res = map.toList.sortBy(_._2).take(5).toMap
    val str = new JSONObject()
    println(str)
  }
}
