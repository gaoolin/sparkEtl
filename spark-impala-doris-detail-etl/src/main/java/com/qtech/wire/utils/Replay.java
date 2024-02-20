package com.qtech.wire.utils;

import com.qtech.etl.config.HadoopConfig;
import com.qtech.etl.config.ImpalaDialect;
import com.qtech.etl.config.SparkConfig;
import com.qtech.etl.utils.SparkDataset2Doris;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static com.qtech.wire.utils.Constant.*;
import static org.apache.spark.sql.functions.col;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/07/11 14:52:40
 * desc   :  用於重跑數據
 */


public class Replay {
    private static final Logger logger = LoggerFactory.getLogger(Replay.class);

    public static void start(String start, String end) {
        SparkConf sparkConf = new SparkConfig("CombineXtremeAndAreoWireUsage Job", "local[*]").initSparkConf();

        SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(ss.sparkContext());

        HadoopConfig.setSparkContextHadoopConf(javaSparkContext);

        javaSparkContext.setLogLevel("WARN");

        // 自定义Spark SQL 方言为 impala，解决 Spark SQL 不支持某些数据类型的问题(72行会报错：value不能转换成long)
        ImpalaDialect impalaDialect = new ImpalaDialect();
        JdbcDialects.registerDialect(impalaDialect);

        Dataset<Row> combinedData = ss.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
//                .option("dbtable", "(" + COMBINE_AREO_WIRE_USAGE_SQL_PREFIX + jobDt + COMBINE_XTREME_WIRE_USAGE_SQL_PREFIX + jobDt + COMBINE_WIRE_USAGE_SQL_SUFFIX + ") tmp")
                .option("dbtable", "(" + COMBINE_AREO_WIRE_USAGE_SQL_PREFIX + start + "' and create_date <= '" + end + COMBINE_XTREME_WIRE_USAGE_SQL_PREFIX + start + "' and create_date <= '" + end + COMBINE_WIRE_USAGE_SQL_SUFFIX + ") tmp")
                .load();

        Dataset<Row> combinedDataReshuffle =
                combinedData.
                        select(col("create_date").cast("timestamp")
                                , col("factory_name")
                                , col("workshop")
                                , col("device_id")
                                , col("prod_type")
                                , col("device_m_id")
                                , col("wire_size").cast("float")
                                , col("machine_no")
                                , col("device_type")
                                , col("wire_cnt")
                                , col("prod_cnt")
                                , col("wire_usage")
                                , col("yield")
                                , col("biz_date")
                                , col("time_period")
                                , col("upload_time")
                        );

        Properties propDoris = new Properties();
        propDoris.setProperty("driver", MYSQL_JDBC_DRIVER);
        propDoris.setProperty("url", DORIS_JDBC_URL);
        propDoris.setProperty("table", RES_DORIS_TABLE_NAME);
        propDoris.setProperty("user", DORIS_USER);
        propDoris.setProperty("password", DORIS_PWD);
        propDoris.setProperty("charset", "UTF-8");

        SparkDataset2Doris save2Doris = new SparkDataset2Doris(combinedDataReshuffle, propDoris, 5000, false);
        long ttl = save2Doris.doInsert();

        ss.close();

        logger.warn(">>>>> 数据插入doris完成，共{}条！", ttl);
    }


    public static void main(String[] args) {
        String start = "2023-09-14T00:00:00";
        String end = "2023-09-01T00:00:00";


        LocalDateTime startDt = LocalDateTime.parse(start);
        LocalDateTime endDt = LocalDateTime.parse(end);

        for (LocalDateTime i = startDt; i.isAfter(endDt); i = i.minusDays(1)) {
            long t1 = System.currentTimeMillis();
            String e = i.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            LocalDateTime j = i.minusDays(1);
            String s = j.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            System.out.println("本次任务开始时间：" + s);
            System.out.println("本次任务结束时间：" + e);
            Replay.start(s, e);
            long t2 = System.currentTimeMillis();

            System.out.println("本次任务用时：" + (t2 - t1) / 1000L / 60L + "分");
        }
    }
}
