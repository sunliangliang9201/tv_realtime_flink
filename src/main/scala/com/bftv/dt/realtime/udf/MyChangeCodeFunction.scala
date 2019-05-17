package com.bftv.dt.realtime.udf

import org.apache.flink.table.functions.ScalarFunction

/**
  * ISO 3166-2省份代码，目的是国家地图展示
  *
  * @author sunliangliang 2019/5/17 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyChangeCodeFunction extends ScalarFunction{

  def eval(province: String): String ={
    province_codes.getOrElse(province, "-")
  }

  val province_codes: Map[String, String] = Map(
    "安徽省" -> "Anhui",
    "北京市" -> "Beijing",
    "重庆省" -> "Chongqing",
    "福建省" -> "Fujian",
    "甘肃省" -> "Gansu",
    "广东省" -> "Guangdong",
    "广西壮族自治区" -> "Guangxi",
    "贵州省" -> "Guizhou",
    "海南省" -> "Hainan",
    "河北省" -> "Hebei",
    "黑龙江省" -> "Heilongjiang",
    "河南省" -> "Henan",
    "湖北省" -> "Hubei",
    "湖南省" -> "Hunan",
    "江苏省" -> "Jiangsu",
    "江西省" -> "Jiangxi",
    "吉林省" -> "Jilin",
    "辽宁省" -> "Liaoning",
    "内蒙古自治区" -> "NeiMongol",
    "宁夏回族自治区" -> "NingxiaHui",
    "青海省" -> "Qinghai",
    "陕西省" -> "Shaanxi",
    "山东省" -> "Shandong",
    "上海市" -> "Shanghai",
    "山西省" -> "Shanxi",
    "四川省" -> "Sichuan",
    "天津市" -> "Tianjin",
    "新疆维吾尔自治区" -> "XinjiangUygur",
    "西藏自治区" -> "Xizang",
    "云南省" -> "Yunnan",
    "浙江省" -> "Zhejiang",
    "台湾" -> "Taiwan",
    "香港" -> "HongKong",
    "澳门" -> "Macao"
  )
}
