package com.qtech.spotChecking.utils

import org.apache.spark.sql.SparkSession

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/04/11 15:56:19
 * desc   :  读取数据
 */


object DataReaderUtil {
  def readMySQL(ss: SparkSession, url: String, userName: String, password: String) = {
    val data = ss.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "select ")
      .option("user", userName)
      .option("password", password)
      .option("driver", "com.mysql.jdbc.Driver")

    data
    }

  def getResultJobRunDt(ss: SparkSession) = {
    val data = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.170.6.40:32120/qtech_wb?binaryTransfer=false&forceBinary=false&reWriteBatchedInserts=true")
      .option("dbtable", "(" + "select max(res_run_dt) as res_run_dt from public.job_run_status" + ") tmp")
      .option("user", "qtech_pg_wb")
      .option("password", "Ft!*NcYe")
      .option("driver", "org.postgresql.Driver")
      .load()

    data
  }
}
