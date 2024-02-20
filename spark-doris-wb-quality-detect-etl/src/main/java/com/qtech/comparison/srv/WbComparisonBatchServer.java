package com.qtech.comparison.srv;


import com.alibaba.fastjson.JSONObject;
import com.qtech.comparison.dpp.batch.WbComparisonBatchEngine;
import com.qtech.etl.exception.biz.comparison.SparkDppException;
import com.qtech.etl.utils.HttpConnectUtils;
import com.qtech.etl.utils.SparkInitConf;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2021/12/21 17:36
 */
@Slf4j
public class WbComparisonBatchServer {

    private static SparkSession spark;

    private void initSpark() throws SparkDppException {
        // 初始话Spark相关配置
        SparkConf sparkConf = SparkInitConf.initSparkConfigs();
        //sparkConf.setMaster("local[*]");
        sparkConf.setAppName("wb comparison")
                .set("spark.default.parallelism", "4")  // 官方推荐，task数量，设置成spark Application 总cpu core数量的2~3倍, cpu = Executor数量 * Executor内核数 = 2 * 2
                .set("spark.sql.analyzer.failAmbiguousSelfJoin", "false");
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    public void run() throws Exception {

        log.warn("=========================Job开始运行===========================");
        initSpark();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        WbComparisonBatchEngine wbComparisonBatchEngine = new WbComparisonBatchEngine(spark);
        wbComparisonBatchEngine.start();

        /* 做单机种测试时，使用以下代码，并修改druid获取数据的代码部分 */
        //WbComparisonBatchEngineTest wbComparisonBatchEngineTest = new WbComparisonBatchEngineTest(spark);
        //wbComparisonBatchEngineTest.start();
        log.warn("=========================Job运行结束===========================");
    }

    public static void main(String[] args) {
        try {
            new WbComparisonBatchServer().run();
        } catch (Exception e) {
            HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobStat", JSONObject.parseObject("{'wb_comparison_result':'0'}"));
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
