package com.qtech.wire.srv;

import com.qtech.etl.config.HadoopConfig;
import com.qtech.etl.config.ImpalaDialect;
import com.qtech.etl.config.SparkConfig;
import com.qtech.etl.utils.SparkDataset2Doris;
import com.qtech.wire.utils.InsertOrUpdate2MySqlBySparkSql;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

import static com.qtech.etl.utils.Constant.IMPALA_JDBC_URL;
import static com.qtech.wire.utils.Constant.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

/**
 * @author : gaozhilin
 * @project : qtech-kudu-wire-usage-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/06 08:21:38
 * @description :
 */


public class CombineXtremeAndAreoWireUsage implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CombineXtremeAndAreoWireUsage.class);

    public void start() {
        SparkConf sparkConf = new SparkConfig("Combine Xtreme And Areo WireUsage Job", "local[*]").initSparkConf();

        SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(ss.sparkContext());

        HadoopConfig.setSparkContextHadoopConf(javaSparkContext);

        javaSparkContext.setLogLevel("WARN");

        // 自定义Spark SQL 方言为 impala，解决 Spark SQL 不支持某些数据类型的问题(72行会报错：value不能转换成long)
        ImpalaDialect impalaDialect = new ImpalaDialect();
        JdbcDialects.registerDialect(impalaDialect);

        ss.read().format("jdbc")
                .option("driver", POSTGRES_JDBC_DRIVER)
                .option("url", POSTGRES_JDBC_URL)
                .option("dbtable", "(" + POSTGRES_TABLE_JOB_DT_SQL + ") tmp")
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PWD)
                .load()
                .createOrReplaceTempView(POSTGRES_TABLE_JOB_DT_VIEW);

        String query_mysql_sql = String.format("select pre_run_time from %s ", POSTGRES_TABLE_JOB_DT_VIEW);

        Dataset<Row> jobDtDs = ss.sql(query_mysql_sql);

        String jobDt = String.valueOf(jobDtDs.select(max(col("pre_run_time")).alias("pre_run_time")).head().get(0));

        logger.warn(">>>>> 本次作业开始时间为：" + jobDt);

        Dataset<Row> combinedData = ss.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
                .option("dbtable", "(" + COMBINE_AREO_WIRE_USAGE_SQL_PREFIX + jobDt + COMBINE_XTREME_WIRE_USAGE_SQL_PREFIX + jobDt + COMBINE_WIRE_USAGE_SQL_SUFFIX + ") tmp")
//                .option("dbtable", "(" + COMBINE_AREO_WIRE_USAGE_SQL_PREFIX + "2023-07-06 00:00:00' and create_date <= '2023-07-07 00:00:00" + COMBINE_XTREME_WIRE_USAGE_SQL_PREFIX + "2023-07-06 00:00:00' and create_date <= '2023-07-07 00:00:00" + COMBINE_WIRE_USAGE_SQL_SUFFIX + ") tmp")
                .load();

        Dataset<Row> nextJobDt = combinedData.select(max(col("create_date")).alias("pre_run_time"));

        Properties prop = new Properties();
        prop.setProperty("driver", POSTGRES_JDBC_DRIVER);
        prop.setProperty("url", POSTGRES_JDBC_URL);
        prop.setProperty("table", POSTGRES_TABLE_WARN);
        prop.setProperty("user", POSTGRES_USER);
        prop.setProperty("password", POSTGRES_PWD);
        /*prop.setProperty("charset", "UTF-8");*/

        InsertOrUpdate2MySqlBySparkSql upsert2Db = new InsertOrUpdate2MySqlBySparkSql(prop, nextJobDt);

        if (nextJobDt != null) {
            upsert2Db.doInsertOrUpdate();
            logger.warn(">>>>> 更新下次作业时间完成！");
        } else {
            logger.warn(">>>>> 下次作业时间不规范！");
            return;
        }

/**
 * 目前测试Spark的API写入MySQL速度太慢，加了参数配置也没什么效果，未能实现分批插入
 */

/*
        combinedData.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("driver", MYSQL_JDBC_DRIVER)
                .option("url", STARROCKS_JDBC_URL)
                .option("dbtable", RES_STARROCKS_TABLE_NAME)
                .option("user", STARROCKS_USER)
                .option("password", STARROCKS_PWD)
                .option("batchsize", 2000)
                .option("isolationLevel", "NONE")
                .save();
*/

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

        Properties propStarRocks = new Properties();
        propStarRocks.setProperty("driver", MYSQL_JDBC_DRIVER);
        propStarRocks.setProperty("url", DORIS_JDBC_URL);
        propStarRocks.setProperty("table", RES_DORIS_TABLE_NAME);
        propStarRocks.setProperty("user", DORIS_USER);
        propStarRocks.setProperty("password", DORIS_PWD);
        propStarRocks.setProperty("charset", "UTF-8");

        SparkDataset2Doris save2StarRocks = new SparkDataset2Doris(combinedDataReshuffle, propStarRocks, 5000, false);
        save2StarRocks.doInsert();
        long cnt = combinedDataReshuffle.count();

        ss.close();

        logger.warn(">>>>> 数据插入 doris 完成，共{}条！", cnt);
    }
}
