package com.qtech.bom.srv;

import com.qtech.etl.config.HadoopConfig;
import com.qtech.etl.config.SparkConfig;
import com.qtech.etl.utils.DateUtils;
import com.qtech.etl.utils.SparkDataset2DbUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.qtech.bom.utils.Constant.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/05/13 10:25:49
 * @description : 作業邏輯
 */


public class Bom {

    private static final Logger logger = LoggerFactory.getLogger(Bom.class);

    public void start() {

        SparkConf sparkConf = new SparkConfig("AA BOM Job", "local[*]").initSparkConf();

        SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(ss.sparkContext());

        HadoopConfig.setSparkContextHadoopConf(javaSparkContext);

        javaSparkContext.setLogLevel("INFO");

        ss.read().format("jdbc")
                .option("driver", ORACLE_JDBC_DRIVER)
                .option("url", ORACLE_JDBC_URL)
                .option("dbtable", "("  + ORACLE_AAB_BOM_SQL + ") tmp")
                .option("user", ORACLE_USER)
                .option("password", ORACLE_PWD)
                .load()
                .createOrReplaceTempView(ORACLE_TABLE_TMP_VIEW);

        Dataset<Row> rs = ss.sql("select * from " + ORACLE_TABLE_TMP_VIEW);

        Dataset<Row> rsTmp = rs.select(col("ID")
                , col("UPN")
                , col("STEPS")
                , col("REPLACE_CPN")
                , col("SOURCE")
                , col("NAME")
                , col("SPEC")
                , col("UNIT")
                , col("QTY")
                , col("WASTE")
                , col("POSITION")
                , col("STATUS").alias("BOM_STATUS")
                , col("TOPLIMIT")
                , col("ID1")
                , col("MDATE")
                , col("MUSER")
                , col("IS_DO")
                , col("REMARKS")
                , col("VERSION")
                , col("OP_CODE")
                , col("SERIES_CODE")
                , col("PART_CODE")
                , col("PART_NAME")
                , col("PART_SPEC")
                , col("TYPE")
                , col("TYPE2")
                , col("PIXEL")
                , col("PROCESS_SPEC")
        ).withColumn("UPDATE_TIME", lit(DateUtils.dateTimeNow()));

        Properties prop = new Properties();
        prop.setProperty("driver", MYSQL_JDBC_DRIVER);
        prop.setProperty("url", STARROCKS_JDBC_URL);
        prop.setProperty("table", RESULT_STARROCKS_TABLE);
        prop.setProperty("user", STARROCKS_USER);
        prop.setProperty("password", STARROCKS_PWD);

        long ttl = SparkDataset2DbUtil.doInsert(rsTmp, prop, 2000);

        ss.close();

        logger.info(">>>>> AA BOM数据插入StarRocks完成，共{}条！", ttl);
    }
}
