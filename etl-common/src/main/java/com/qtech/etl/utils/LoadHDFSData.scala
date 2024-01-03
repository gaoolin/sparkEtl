package com.qtech.etl.utils

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.matching.Regex

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/05/30 11:57:43
 * desc   :  根据路径读取HDFS上的文件，读取文件可以是 csv, json, parquet, jdbc
 */

// Scala没有静态方法或静态字段，可以使用object这个语法达到相同的目的，对象定义了某个类的单个实例
object LoadHDFSData {


  def getDataRDD(filePath: String, ss: SparkSession): RDD[Row] = {

    val df: DataFrame = getDF(filePath, ss)
    df.rdd
  }

  def getDataDF(filePath: String, ss: SparkSession): DataFrame = {
    val df: DataFrame = getDF(filePath, ss)
    df
  }

  protected def readByCsv(path: String, ss: SparkSession): DataFrame = {
    val df: DataFrame = ss.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(path)
    df
  }

  protected def readByParquet(path: String, ss: SparkSession): DataFrame = {
    val df: DataFrame = ss.read.format("parquet").load(path)
    df
  }

  protected def readByJDBC(url: String, tableName: String, prop: Properties, ss: SparkSession): DataFrame = {
    val df: DataFrame = ss.read.jdbc(url, tableName, prop)
    df
  }

  protected def readByJson(filePath: String, ss: SparkSession): DataFrame = {
    val df: DataFrame = ss.read.json(filePath)
    df
  }

  protected def getDF(filePath: String, ss: SparkSession): DataFrame = {
    val pattern: Regex = ".*\\.".r
    val fileType: String = pattern.replaceAllIn(filePath, "")
    val df: DataFrame = fileType match {
      case "csv" => readByCsv(filePath, ss)
      case "parquet" => readByParquet(filePath, ss)
      case "json" => readByJson(filePath, ss)
      case _ => throw new IllegalArgumentException("请检查文件类型，文件类型需为：【csv/parquet/json】")
    }
    df
  }
}
