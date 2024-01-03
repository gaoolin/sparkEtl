package com.qtech.etl.utils

import com.qtech.etl.config.HadoopConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/05/26 16:48:21
 * desc   :  判断本地或者hdfs上是否存在某个文件
 */

// Scala没有静态方法或静态字段，可以使用object这个语法达到相同的目的，对象定义了某个类的单个实例
object FilePath {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val hdfs: FileSystem = FileSystem.get(HadoopConfig.setHadoopConf(new Configuration()))

  private val local: FileSystem = FileSystem.get(new Configuration)

  /**
   *
   * @param filePath 文件的绝对路径
   * @param pathType 路径类型：local为本地文件， hdfs为HDFS文件
   * @return
   */
  def isExist(filePath: String, pathType: String = "local"): Boolean = {

    var bool = false
    val regex: Regex = "[^/:\\\\\\\\]+$".r
    regex.findFirstMatchIn(filePath) match {
      case Some(fileName) => {
        if (pathType == "hdfs") {
          bool = hdfs.exists(new Path(filePath))
          logger.info(">>>>> 文件 " + fileName.group(0).replace("/", "") + " ，在HDFS上{}！", if (bool) "存在" else "不存在")
        } else if (pathType == "local") {
          bool = local.exists(new Path(filePath))
          logger.info(">>>>> 文件 " + fileName.group(0).replace("\\", "") + " ，在本地{}！", if (bool) "存在" else "不存在")
        } else {
          logger.info(">>>>> 路径类型参数错误，请检查！")
        }
      }
      case None => logger.info(">>>>> 未匹配到文件名，或文件名不符合规范， 请检查文件名！")
    }
    bool
  }
}
