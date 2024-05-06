package com.qtech.spotChecking.dpp.batch

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/04/11 15:31:18
 * desc   :
 */


abstract class BatchEngine {

  def start(): Unit =
    this.srvProcessData()

  def srvProcessData(): Unit
}
