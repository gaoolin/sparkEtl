package com.qtech.baseinfo.dpp;

import com.qtech.baseinfo.dpp.batch.BatchEngine;
import com.qtech.etl.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static com.qtech.baseinfo.utils.Constants.*;
import static org.apache.spark.sql.functions.lit;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/01/02 11:18:27
 * desc   :
 */

@Slf4j
public class BaseInfoBatchEngine extends BatchEngine {

    private final SparkSession spark;

    public BaseInfoBatchEngine(SparkSession spark) {
        this.spark = spark;
    }
    @Override
    protected void srvProcessData() throws Exception {

        Dataset<Row> df = spark.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
                .option("dbtable", "ods_baseinfo.ems_t_device_calcgd1jh1u82gwwionk")
                .load();
        log.info("已获取ems_t_device表数据");
        df.withColumn("update_time", lit(DateUtils.dateTimeNow()))
                .write().format("jdbc")
                .option("driver",DORIS_JDBC_DRIVER)
                .option("url", DORIS_JDBC_URL)
                .option("dbtable", "qtech_eq_ods.ems_t_device")
                .option("user", "root")
                .option("password", "")
                .mode(SaveMode.Append)
                .save();

        /* Connect to 10.244.4.106:8040 [/10.244.4.106] failed: Connection timed out: connect*/
        /*df.write().format("doris")
                .option("doris.table.identifier", "qtech_eq_ods.ems_t_device")
                .option("doris.fenodes", DORIS_FE_IP + ":" + DORIS_FE_PORT)
                .option("user", "root")
                .option("password", "")
                //其它选项
                //指定你要写入的字段
                //.option("doris.write.fields", "$YOUR_FIELDS_TO_WRITE")
                // 支持设置 Overwrite 模式来覆盖数据
                //.mode(SaveMode.Overwrite)
                .save();*/
        log.info("已更新ems_t_device数据");

        Dataset<Row> dfBox = spark.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
                .option("dbtable", "ods_baseinfo.ems_t_pbox_info")
                .load();
        log.info("已获取ems_t_pbox_info表数据");
        dfBox.withColumn("update_time", lit(DateUtils.dateTimeNow()))
                .write().format("jdbc")
                .option("driver", DORIS_JDBC_DRIVER)
                .option("url", DORIS_JDBC_URL)
                .option("dbtable", "qtech_eq_ods.ems_t_pbox_info")
                .option("user", "root")
                .option("password", "")
                .mode(SaveMode.Append)
                .save();
        log.info("已更新ems_t_pbox_info数据");

        Dataset<Row> dfCode = spark.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
                .option("dbtable", "ods_baseinfo.ems_v_code_name")
                .load();
        log.info("已获取ems_v_code_name表数据");
        dfCode.withColumn("update_time", lit(DateUtils.dateTimeNow()))
                .drop("hierarchy_show_code")
                .write().format("jdbc")
                .option("driver", DORIS_JDBC_DRIVER)
                .option("url", DORIS_JDBC_URL)
                .option("dbtable", "qtech_eq_ods.ems_v_code_name")
                .option("user", "root")
                .option("password", "")
                .mode(SaveMode.Append)
                .save();
        log.info("已更新ems_v_code_name数据");

        Dataset<Row> dfLot = spark.read().format("jdbc")
                .option("driver", IMPALA_JDBC_DRIVER)
                .option("url", IMPALA_JDBC_URL)
                .option("dbtable", "ods_baseinfo.ems_t_collector_program")
                .load();
        log.info("已获取ems_t_collector_program表数据");
        dfLot.withColumn("update_time", lit(DateUtils.dateTimeNow()))
                .write().format("jdbc")
                .option("driver", DORIS_JDBC_DRIVER)
                .option("url", DORIS_JDBC_URL)
                .option("dbtable", "qtech_eq_ods.ems_t_collector_program")
                .option("user", "root")
                .option("password", "")
                .mode(SaveMode.Append)
                .save();
        log.info("已更新ems_t_collector_program数据");
    }
}
