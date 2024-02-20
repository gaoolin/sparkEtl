package com.qtech.baseinfo.job;

import com.qtech.baseinfo.dpp.BaseInfoBatchEngine;
import com.qtech.etl.exception.biz.comparison.SparkDppException;
import com.qtech.etl.utils.SparkInitConf;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/01/02 09:14:51
 * desc   :
 */

@Slf4j
public class EtlJob {

    private static SparkSession spark;

    private void initSpark() throws SparkDppException {
        SparkConf sparkConf = SparkInitConf.initSparkConfigs();
        //sparkConf.setMaster("local[*]");
        sparkConf.setAppName("base information etl")
                .set("spark.default.parallelism", "4")
                // spark.yarn.jars 传入的 HDFS 下需要包含 spark 相关的原始jar包，如果只包含依赖jar包而没有spark原始依赖则无法提交任务，因为spark.core都找不到了，AM无法创建
                //.set("spark.yarn.jars", "hdfs:///spark-jars/spark-doris-connector-3.3_2.12-1.3.0.jar")
                .set("spark.debug.maxToStringFields", "100") // 默认25个字段，否则会提示字段过多
                .set("spark.sql.analyzer.failAmbiguousSelfJoin", "false");
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        log.warn("Spark初始化集群配置已完成！");
    }

    public void run() throws Exception {
        log.warn("Job开始运行");
        initSpark();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        new BaseInfoBatchEngine(spark).start();
        log.warn("Job运行结束");

    }

    public static void main(String[] args) {
        try {
            new EtlJob().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
