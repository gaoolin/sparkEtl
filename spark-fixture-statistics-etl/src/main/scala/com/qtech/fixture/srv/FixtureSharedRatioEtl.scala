package com.qtech.fixture.srv

import java.time.LocalDateTime
import java.time.temporal.WeekFields
import java.util.Properties

import com.qtech.etl.config.{HadoopConfig, SparkConfig}
import com.qtech.etl.utils.{Constants, HttpUtils}
import com.qtech.fixture.exception.DataExistException
import com.qtech.fixture.utils.Constants._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/02/29 11:47:31
 * desc   :
 */

object FixtureSharedRatioEtl extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
//  val sparkConf: SparkConf = new SparkConfig("Fixture Shared Ratio Etl", "local[*]").initSparkConf
    val sparkConf: SparkConf = new SparkConfig("Fixture Shared Ratio Etl").initSparkConf

  val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc: SparkContext = ss.sparkContext
  HadoopConfig.setSparkContextHadoopConf(sc)
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val t1: Long = System.currentTimeMillis()

    ss.read.format("jdbc").option("driver", MYSQL_JDBC_DRIVER).option("url", MYSQL_JDBC_URL).option("dbtable", "(" + MYSQL_DATE_FLAG + ") tmp").option("user", MYSQL_USER).option("password", MYSQL_PWD).load.createOrReplaceTempView("tmp")
    val frame: DataFrame = ss.sql("select * from tmp")
    val flag: Int = frame.rdd.first().getInt(0)
    logger.warn(">>>>> 作业运行状态为：{}。", if (flag == 0) "继续" else "停止")
    // 1:相等，0：不相等
    if (flag == 0) {
      val s: String = HttpUtils.sendGet("http://10.170.6.40:31735/fixture/statistics/line", Constants.GBK)

      // json数字字符串转换成 Dataset的机种方式，特别注意在提取schema的时候指定的数据类型，否则会出现提取后的数据为空。
      /*
      val dfJson: DataFrame = ss.sql(s"""select '$s' as sch""")
      val dfJson1: DataFrame = dfJson.select(from_json($"sch", ArrayType(StructType(StructField("week_num", StringType) :: StructField("dept_id", IntegerType) :: StructField("p_cnt", FloatType) :: StructField("m_cnt", FloatType) :: StructField("shared_ratio", FloatType) :: Nil))).alias("sch"))
      dfJson1.show(false)

      val dfJson2: DataFrame = ss.sql(s"""select explode(from_json('$s', 'ARRAY<STRUCT<week_num: STRING, dept_id: int, p_cnt: float, m_cnt: float, shared_ratio: float>>')) as sch;""")
      dfJson2.show(false)
      */

      val sqlStr: String =
        s"""
           |with dfJson as (select '$s' as sch),
           |dfJson1 as (select explode(from_json(sch, 'array< struct<`week_num`: string, `dept_id`: int, `p_cnt`: float, `m_cnt`: float, `shared_ratio`: float > >')) as sch from dfJson)
           |select sch.week_num, sch.dept_id, sch.p_cnt, sch.m_cnt, sch.shared_ratio from dfJson1;
           |""".stripMargin

      val dfJson3: DataFrame = ss.sql(sqlStr)

      //      dfJson3.show(false)

      // val weekStr: String = dfJson3.select(col("week_num")).rdd.first().getString(0)

      //      dfJson3.rdd.map(x => {
      //        x._0
      //      })


      // 获取当前时间
      val now: LocalDateTime = LocalDateTime.now()

      val pre_week_dt: LocalDateTime = now.minusDays(1)
      val week_num: String = pre_week_dt.get(WeekFields.ISO.weekOfYear).toString
      val year: String = pre_week_dt.getYear.toString

      val weekStr = s"""$year-$week_num"""

      //      val calendar: Calendar = Calendar.getInstance
      //      val week_num: Int = calendar.get(Calendar.WEEK_OF_YEAR)
      //      System.out.println("第几周ByCalendar=" + week_num)

      //      val splitStr: Array[String] = weekStr.split("-", 2)
      //      val week: Int = Integer.parseInt(splitStr(1))


      val dfJson4: DataFrame = dfJson3.filter(s"""week_num = '$weekStr'""")
      //      dfJson4.show(false)

      /*
        默认为SaveMode.ErrorIfExists模式，该模式下，若数据库中已经存在该表，则会直接报异常，导致数据不能存入数据库；
        SaveMode.Append 若表已经存在，则追加在该表中；若该表不存在，则会先创建表，再插入数据；
        SaveMode.Overwrite 重写模式，其本质是先将已有的表及其数据全都删除，再重新创建该表，然后插入新的数据；
        SaveMode.Ignore 若表不存在，则创建表，并存入数据；若表存在的情况下，直接跳过数据的存储，不会报错
        */

      val prop = new Properties
      prop.setProperty("user", MYSQL_USER)
      prop.setProperty("password", MYSQL_PWD)
      prop.setProperty("driver", MYSQL_JDBC_DRIVER)
      prop.setProperty("charset", "UTF-8")

      val isExist: Dataset[Row] = ss.read.jdbc(MYSQL_JDBC_URL, "fixture_statistics", prop).filter(s"""week_num = '$weekStr'""")
      //      isExist.show(false)

      try {
        if (isExist.count() > 0) {
          throw new DataExistException("MySQL存在当前周次数据，将不操作数据库！")
          //          logger.warn("MySQL存在当前周次数据，将不操作数据库！")
        } else {
          dfJson4.write.mode(SaveMode.Append).jdbc(MYSQL_JDBC_URL, "fixture_statistics", prop)
        }
      } catch {
        case e: DataExistException => {
          //          e.printStackTrace()
          //          "-1"
          logger.warn(">>>>> MySQL存在当前周次数据，将不操作数据库！")

        }
      } finally {
        ss.stop()
      }
      val t2: Long = System.currentTimeMillis()
      logger.warn(">>>>> 本次作业耗时：{}秒。", math.round((t2 - t1) / 1000.0))
    }
  }
}
