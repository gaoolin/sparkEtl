package com.qtech.spotChecking.utils

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/04/11 15:49:22
 * desc   :  作业运行工具类
 */


object JobRunService {

  // 属性
  var resRunDt: String = _
  var rawRunDt: String = _
  var stat: String = _

  def checkJobRunCondition(jobName: String): Unit = {
    val jobNameList = List("WbOlpSpotCheck")
    if (!jobNameList.contains(jobName)) {
      throw new Exception()
    }
  }
}
