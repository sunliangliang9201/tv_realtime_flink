package com.bftv.dt.realtime.utils

import java.util.BitSet
import scala.util.hashing.MurmurHash3


/**
  * 布隆过滤器
  *
  * @author sunliangliang 2019-04-01 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyBloomFilter {

  //位数组的大小，存放hash值
  val bitSetSize = 1 << 22

  //位数组
  val bitSet = new BitSet(bitSetSize)

  //传入murmurhash中的seed的范围
  val seedNum = 4

  /**
    * 使用murmurhash中的hash，将bitset中填入该元素多个hash计算出来的值
    * @param str key string
    */
  def hashValue(str: String): Unit ={
    if(str != null && !str.isEmpty){
      for (i <- 1 to seedNum){
        bitSet.set(Math.abs(MurmurHash3.stringHash(str, i)) % bitSetSize, true)
      }
    }
  }

  /**
    * 检查str是否已经存在
    * @param str key string
    * @return true or false
    */
  def exists(str: String): Boolean ={
    if(str == null || str.isEmpty){
      return true //防止多个为空或者null的uuid被认为是多个不同用户
    }else{
      for (i <- 1 to seedNum){
        if (!bitSet.get(Math.abs(MurmurHash3.stringHash(str, i)) % bitSetSize)){
          return false
        }
      }
    }
    return true
  }

}
