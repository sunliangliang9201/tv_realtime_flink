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
//    "安徽省" -> "Anhui",
//    "北京市" -> "Beijing",
//    "重庆省" -> "Chongqing",
//    "福建省" -> "Fujian",
//    "甘肃省" -> "Gansu",
//    "广东省" -> "Guangdong",
//    "广西壮族自治区" -> "Guangxi",
//    "贵州省" -> "Guizhou",
//    "海南省" -> "Hainan",
//    "河北省" -> "Hebei",
//    "黑龙江省" -> "Heilongjiang",
//    "河南省" -> "Henan",
//    "湖北省" -> "Hubei",
//    "湖南省" -> "Hunan",
//    "江苏省" -> "Jiangsu",
//    "江西省" -> "Jiangxi",
//    "吉林省" -> "Jilin",
//    "辽宁省" -> "Liaoning",
//    "内蒙古自治区" -> "NeiMongol",
//    "宁夏回族自治区" -> "NingxiaHui",
//    "青海省" -> "Qinghai",
//    "陕西省" -> "Shaanxi",
//    "山东省" -> "Shandong",
//    "上海市" -> "Shanghai",
//    "山西省" -> "Shanxi",
//    "四川省" -> "Sichuan",
//    "天津市" -> "Tianjin",
//    "新疆维吾尔自治区" -> "XinjiangUygur",
//    "西藏自治区" -> "Xizang",
//    "云南省" -> "Yunnan",
//    "浙江省" -> "Zhejiang",
//    "台湾" -> "Taiwan",
//    "香港" -> "HongKong",
//    "澳门" -> "Macao"
    "安徽省" -> "CN-34",
    "北京市" -> "CN-11",
    "重庆省" -> "CN-50",
    "福建省" -> "CN-35",
    "甘肃省" -> "CN-62",
    "广东省" -> "CN-44",
    "广西壮族自治区" -> "CN-45",
    "贵州省" -> "CN-52",
    "海南省" -> "CN-46",
    "河北省" -> "CN-13",
    "黑龙江省" -> "CN-23",
    "河南省" -> "CN-41",
    "湖北省" -> "CN-42",
    "湖南省" -> "CN-43",
    "江苏省" -> "CN-32",
    "江西省" -> "CN-36",
    "吉林省" -> "CN-22",
    "辽宁省" -> "CN-21",
    "内蒙古自治区" -> "CN-15",
    "宁夏回族自治区" -> "CN-64",
    "青海省" -> "CN-63",
    "陕西省" -> "CN-61",
    "山东省" -> "CN-37",
    "上海市" -> "CN-31",
    "山西省" -> "CN-14",
    "四川省" -> "CN-51",
    "天津市" -> "CN-12",
    "新疆维吾尔自治区" -> "CN-65",
    "西藏自治区" -> "CN-54",
    "云南省" -> "CN-53",
    "浙江省" -> "CN-33",
    "台湾" -> "CN-71",
    "香港" -> "CN-91",
    "澳门" -> "CN-92"
  )
}
