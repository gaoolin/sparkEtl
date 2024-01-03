package com.qtech.comparison.utils

import java.sql.{Connection, DriverManager, Statement}
import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.qtech.etl.utils.HttpConnectUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/10/16 11:35:16
 * desc   :  通过JDBC插入数据库
 */


class Persist() {

  def doPost(df: Dataset[Row]): Boolean = {
    var flag = true
    df.rdd.foreachPartition((iter: Iterator[Row]) => {
      iter.foreach((r: Row) => {
        val simId: String = r.getString(0)
        val code: String = r.getInt(3) + ""
        val desc: String = r.getString(4)
        val redisVal: String = code + "*" + desc
        val dict: util.Map[String, String] = Map(simId -> redisVal).asJava
        val jsonObject: JSONObject = JSON.parseObject(JSON.toJSONString(dict, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullStringAsEmpty))
        val post: String = HttpConnectUtils.post("http://10.170.6.40:32767/comparison/api/updateRes", jsonObject)
        flag = flag & (if (post == "0") true else false)
      })
    })
    flag
  }

  def doDorisInsert(df: Dataset[Row], url: String, user: String, password: String, tbName: String): Unit = {

    df.rdd.foreachPartition((iter: Iterator[Row]) => {
      // 建立JDBC连接
      Class.forName("com.mysql.cj.jdbc.Driver").newInstance
      val conn: Connection = DriverManager.getConnection(url + "&user=" + user + "&password=" + password)
      val stmt: Statement = conn.createStatement()

      // 批量写入数据
      iter.foreach((r: Row) => {
        val simId: String = r.getString(0)
        val programName: String = r.getString(1)
        val dt: String = r.getString(2)
        val code: String = r.getInt(3) + ""
        val desc: String = r.getString(4)
        val sql = s"insert into $tbName values ('$simId', '$programName', '$dt', '$code', '$desc')"
        stmt.addBatch(sql)
      })
      stmt.executeBatch()
      stmt.close()
      conn.close()
    })
  }

  def doPostgresUpsert(df: Dataset[Row], url: String, user: String, password: String, tbName: String): Unit = {
    // 建立JDBC连接

    df.rdd.foreachPartition((iter: Iterator[Row]) => {
      // 建立JDBC连接
      Class.forName("org.postgresql.Driver").newInstance
      val conn: Connection = DriverManager.getConnection(url + "&user=" + user + "&password=" + password)
      val stmt: Statement = conn.createStatement()

      // 批量写入数据
      iter.foreach((r: Row) => {
        val simId: String = r.getString(0)
        val programName: String = r.getString(1)
        val dt: String = r.getString(2)
        val code: String = r.getInt(3) + ""
        val desc: String = r.getString(4)
        val sql =
          s"""
             |insert into $tbName values ('$simId', '$programName', '$dt', '$code', '$desc')
             |ON CONFLICT ON CONSTRAINT eq_control_info_unique_key
             |DO UPDATE SET dt = '$dt', code = '$code', description = '$desc';
             |""".stripMargin
        stmt.addBatch(sql)
      })
      stmt.executeBatch()
      stmt.close()
      conn.close()
    })
  }
}
