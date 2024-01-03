package com.qtech.comparison.utils

import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.qtech.etl.utils.HttpConnectUtils
import org.junit.Test

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/10/17 10:51:37
 * desc   :  TODO
 */

@Test
class ScalaTest {

  @Test
  def TestRedis(): Unit={

    val dict: util.Map[String, String] = Map("simId" -> "aa").asJava
    val jsonObject: JSONObject = JSON.parseObject(JSON.toJSONString(dict, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullStringAsEmpty))

    val post: String = HttpConnectUtils.post("http://10.170.6.40:32767/comparison/api/updateRes", jsonObject)
//    val post: String = HttpConnectUtils.post("http://localhost:8080/comparison/api/updateRes", jsonObject)

    println(post)

  }
}
