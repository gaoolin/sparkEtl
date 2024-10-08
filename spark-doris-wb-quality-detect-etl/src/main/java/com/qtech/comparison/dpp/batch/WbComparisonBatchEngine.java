package com.qtech.comparison.dpp.batch;

import com.alibaba.fastjson.JSONObject;
import com.qtech.comparison.dpp.data.DataParse;
import com.qtech.comparison.dpp.data.DataProcess;
import com.qtech.comparison.srv.JobRunService;
import com.qtech.comparison.utils.ComparisonInfo;
import com.qtech.comparison.utils.Persist;
import com.qtech.etl.exception.biz.comparison.SparkDppException;
import com.qtech.etl.utils.DateUtils;
import com.qtech.etl.utils.HttpConnectUtils;
import com.qtech.etl.utils.PropertiesManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.sql.SQLException;
import java.util.*;

import static com.qtech.comparison.algorithm.Algorithm.*;
import static com.qtech.comparison.utils.Utils.*;
import static org.apache.spark.sql.functions.*;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2022/06/12 10:16:07
 * desc   :  业务逻辑类
 */

@Slf4j
public class WbComparisonBatchEngine extends BatchEngine {

    private final SparkSession spark;

    public WbComparisonBatchEngine(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    protected void srvProcessData() throws SQLException, SparkDppException, ClassNotFoundException {

        long timeStart = System.currentTimeMillis();

        Boolean continueOrNot = JobRunService.checkJobRunCondition();

        if (!continueOrNot) {
            return;
        }

        Map<String, String> finalJobRunDt = JobRunService.getFinalJobRunDt();
        String resRunDt = finalJobRunDt.get("wb_comparison_result");
        String rawRunDt = finalJobRunDt.get("wb_comparison_raw");

        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobStat", JSONObject.parseObject("{'wb_comparison_result':'1'}"));

        PropertiesManager.loadProp("WbComparison.properties");
        PropertiesManager pm = PropertiesManager.getInstance();
        log.warn("=============================配置文件已加载====================================");

        String power = pm.getString("power");
        String dorisDriver = pm.getString("jdbc.doris.driver");
        String dorisUrl = pm.getString("jdbc.doris.url");
        String dorisUser = pm.getString("jdbc.doris.user");
        String dorisPwd = pm.getString("jdbc.doris.pwd");
        String postgresDriver = pm.getString("jdbc.postgres.driver");
        String postgresUrl = pm.getString("jdbc.postgres.url");
        String postgresUser = pm.getString("jdbc.postgres.user");
        String postgresPwd = pm.getString("jdbc.postgres.pwd");
        String coordinationDetailTb = pm.getString("res.store.table.comparison.detail");

        String comparisonInfoTb = pm.getString("res.store.table.comparison.info");
        String comparisonDetailTb = pm.getString("res.store.table.detail");
        String ctrlTb = power.equals("0") ? pm.getString("res.store.table") : pm.getString("res.store.table.test");

        long timeSparkContext = System.currentTimeMillis();

        Dataset<Row> stdModels = getStdModels(spark, postgresDriver, postgresUrl, postgresUser, postgresPwd);

        if (stdModels.isEmpty()) {
            throw new SparkDppException("没有标准模板数据，无法进行比对任务，请检查模板数据，程序将退出！");
        }

        Dataset<Row> stdModWireCnt = stdModels.groupBy("std_mc_id")
                .agg(count("std_mc_id"))
                .withColumnRenamed("count(std_mc_id)", "std_mod_line_cnt");

        long timeStdModels = System.currentTimeMillis();
        log.warn(">>>> fetch stdModels spend {} seconds.",
                Math.round((timeStdModels - timeSparkContext) / ComparisonInfo.SECONDS_UNIT.getFlt()));

        String endDate = DateUtils.dateTimeNow("yyyy-MM-dd HH:mm:ss");
        String currentRunDt = offsetTime(rawRunDt, ComparisonInfo.RUNTIME_LAG_MINUTES.getNum());
        String comparisonBeginDate = judgeRunDt(currentRunDt, endDate);  // 判定是否起始时间超长
        log.warn(">>>> comparison dt range: {} - {}.", comparisonBeginDate, endDate);

        Dataset<Row> comparisonDf = DataParse.druidParse(spark, comparisonBeginDate).persist(StorageLevel.MEMORY_AND_DISK()); // 避免惰性运行，引起的时间计算不一致问题

        String maxDtOfRawDf = getMaxDtAndOffset(comparisonDf);

        // rawDf = rawDf.sample(0.1);

        // Dataset<Row> comparisonDf = rawDf.filter(String.format("dt > '%s'", comparisonBeginDate));

        Dataset<Row> processedDf = DataProcess.doProcess(comparisonDf, stdModWireCnt)
                .where("mcs_by_pieces_index = 1").persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> noProgDf = noProg(processedDf.where("std_mod_line_cnt is null"));

        Dataset<Row> lackLnDa = processedDf.filter("check_port < std_mod_line_cnt or check_port = std_mod_line_cnt + 1");

        Dataset<Row> fullLnDa = processedDf.filter("check_port = std_mod_line_cnt and check_port = cnt");

        Dataset<Row> linkLnDa = processedDf.filter("check_port = std_mod_line_cnt + 2 and check_port = cnt");

        Dataset<Row> overLnDa = processedDf.filter("check_port = std_mod_line_cnt + 1 or check_port > std_mod_line_cnt + 2");

        Dataset<Row> lackLn2doc = lackLn2doc(lackLnDa);

        Dataset<Row> overLn2doc = overLn2doc(overLnDa);

        Dataset<Row> linkLn2FullLn = linkLnEstimate(linkLnDa);

        Dataset<Row> fullLnTtl = fullLnDa.select(col("sim_id"), col("mc_id"), col("dt"),
                col("first_draw_time"), col("line_no"), col("lead_x"), col("lead_y"),
                col("pad_x"), col("pad_y"), col("check_port"), col("pieces_index"),
                col("sub_mc_id"), col("cnt"), col("wire_len"))
                .union(linkLn2FullLn)
                .sort(col("sim_id"), col("mc_id"), col("first_draw_time"), col("pieces_index"), col("line_no"));

        // 自排序算法
        /* Dataset<Row> normalizeUnitDf = coordinateNormalization(fullLnTtl); */

        // normalizeUnitDf.show(showCount);

        /* Dataset<Row> normalizeDf = generateLineNmb(normalizeUnitDf); */

        // normalizeDf.show(showCount);

        Dataset<Row> fullLnDiff = diff(fullLnTtl);

        Dataset<Row> fullLnSta = fullLnMkSta(fullLnDiff, stdModels);

        Dataset<Row> fullLn2doc = fullLn2doc(fullLnSta);

        Dataset<Row> ttlLn = DataProcess.unionDataFrame(new ArrayList<>(Arrays.asList(noProgDf, lackLn2doc, fullLn2doc, overLn2doc))).withColumn("program_name", lit("wb_comparison"));

        /* 过滤机型 */
        ttlLn.createOrReplaceTempView("ttlLn");
        getNeedFilterMcId(spark, postgresDriver, postgresUrl, postgresUser, postgresPwd).createOrReplaceTempView("needFilterMcId");

        Dataset<Row> needModifyDf = spark.sql("select * from ttlLn where mc_id in (select mc_id from needFilterMcId)");
        Dataset<Row> leaveDf = spark.sql("select * from ttlLn where mc_id not in (select mc_id from needFilterMcId)");

        needModifyDf.withColumn("code", lit(0));
        needModifyDf.withColumn("description", lit("Invalidation"));

        Dataset<Row> ctrlInfoDfAll = leaveDf.union(needModifyDf);
//        ctrlInfoDfAll.show(false);

        Dataset<Row> ctrlInfoDf = ctrlInfoDfAll.select(col("sim_id"), col("program_name"), col("dt"), col("code"), col("description"));

        String maxDtOfResDf = getMaxDtAndOffset(ctrlInfoDf);

        boolean flag = new Persist().doPost(ctrlInfoDf);
        if (!flag) {
            log.error("写入redis部分或者全部出错！");
        }

        Properties postgresProp = new Properties();
        postgresProp.put("user", postgresUser);
        postgresProp.put("password", postgresPwd);

        /* --比对结果入postgres */
        upsertPostgres(ctrlInfoDf, postgresUrl, postgresProp);
        log.warn(">>>> upsert postgresql result done.");

        // update job run dt
        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobRunDt", JSONObject.parseObject("{'wb_comparison_result':'" + maxDtOfResDf + "'}"));
        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobRunDt", JSONObject.parseObject("{'wb_comparison_raw':'" + maxDtOfRawDf + "'}"));
        log.warn(">>>> update job run dt done.");

        Dataset<Row> appendDf = comparisonDf.select(col("sim_id"), col("mc_id"), col("dt"), col("line_no"), col("lead_x"), col("lead_y"), col("pad_x"), col("pad_y"), col("check_port"),
                col("pieces_index")).where(String.format("`dt` > '%s'", rawRunDt)).withColumn("load_time", lit(DatetimeBuilder("yyyy-MM-dd HH:mm:ss")));
        /* --坐标明细入库 */
        sav2Doris(appendDf, dorisDriver, dorisUrl, dorisUser, dorisPwd, coordinationDetailTb);
        log.warn(">>>> insert to doris raw data done.");

        /* --比对明细入库 */
        Dataset<Row> comparisonResultDetail = ttlLn.filter(String.format("dt > '%s'", resRunDt));
        sav2Doris(comparisonResultDetail, dorisDriver, dorisUrl, dorisUser, dorisPwd, comparisonDetailTb);
        Dataset<String> json = comparisonResultDetail.toJSON();
        json.foreach(row -> {
            System.out.println(row);
            JSONObject jsonObject = JSONObject.parseObject(row);
            String i = HttpConnectUtils.post("http://10.170.6.40:30864/rabbitmq/msg/wbComparison", jsonObject);
        });
        log.warn(">>>> insert to doris comparison detail data done.");


        comparisonDf.unpersist();
        processedDf.unpersist();

        /* --更新比对时间 */
        updateJobRunDt("wb_comparison_result", maxDtOfResDf);
        updateJobRunDt("wb_comparison_raw", maxDtOfRawDf);
        log.warn("update job run datetime on database done.");

        long timeEnd = System.currentTimeMillis();
        log.warn(">>>> job run spend {} s.", Math.round((timeEnd - timeStart) / ComparisonInfo.SECONDS_UNIT.getFlt()));
    }
}
