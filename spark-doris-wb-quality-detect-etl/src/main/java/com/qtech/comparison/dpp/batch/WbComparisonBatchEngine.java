package com.qtech.comparison.dpp.batch;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.qtech.comparison.algorithm.Algorithm;
import com.qtech.comparison.dpp.data.DataParse;
import com.qtech.comparison.dpp.data.DataProcess;
import com.qtech.comparison.utils.ComparisonInfo;
import com.qtech.etl.utils.HttpConnectUtils;
import com.qtech.comparison.utils.Persist;
import com.qtech.etl.exception.biz.comparison.SparkDppException;
import com.qtech.etl.utils.PropertiesManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

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

        String stat = HttpConnectUtils.get("http://10.170.6.40:32767/job/api/getJobStat/wb_comparison_result");

        if ("1".equals(stat)) {
            log.warn("已有打线图作业正在运行，本次作业将退出！");
            return;  // 退出此方法
        }
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

        String druidBeginDate;
        String preRunDt;

        preRunDt = HttpConnectUtils.get("http://10.170.6.40:32767/job/api/getJobRunDt/wb_comparison_result");
        druidBeginDate = HttpConnectUtils.get("http://10.170.6.40:32767/job/api/getJobRunDt/wb_comparison_raw");

        // preRunDt = getJobRunDt("wb_comparison_result");  // 2023-09-21 14:25:34
        //druidBeginDate = getJobRunDt("wb_comparison_raw");  // 2023-09-21 14:40:34

        if (Strings.isNullOrEmpty(preRunDt) || Strings.isNullOrEmpty(druidBeginDate)) {
            throw new SparkDppException("作业的数据时间不能为空，请检查！");
        }

        String endDate = DatetimeBuilder("yyyy-MM-dd HH:mm:ss");  // "2021-12-28 08:20:30";
        String comparisonBeginDate = judgeRunDt(preRunDt, endDate);  // 2023-09-21 14:25:34

        log.warn(">>>> comparison dt range: {} - {}.", comparisonBeginDate, endDate);

        Dataset<Row> comparisonDF = DataParse.druidParse(spark, comparisonBeginDate).persist(StorageLevel.MEMORY_AND_DISK()); // 避免惰性运行，引起的时间计算不一致问题

        String maxDtOfRawDF = getNextJobRunDt(comparisonDF);

        String nextRunDt = offsetTime(maxDtOfRawDF, ComparisonInfo.RUNTIME_LAG_MINUTES.getNum());

        log.warn(">>>> next job run dt: {}.", nextRunDt);

        // rawDF = rawDF.sample(0.1);

        // Dataset<Row> comparisonDF = rawDF.filter(String.format("dt > '%s'", comparisonBeginDate));

        Dataset<Row> processedDF = DataProcess.doProcess(comparisonDF, stdModWireCnt)
                .where("mcs_by_pieces_index = 1").persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> noProgDF = Algorithm.noProg(processedDF.where("std_mod_line_cnt is null"));

        Dataset<Row> lackLnDa = processedDF.filter("check_port < std_mod_line_cnt or check_port = std_mod_line_cnt + 1");

        Dataset<Row> fullLnDa = processedDF.filter("check_port = std_mod_line_cnt and check_port = cnt");

        Dataset<Row> linkLnDa = processedDF.filter("check_port = std_mod_line_cnt + 2 and check_port = cnt");

        Dataset<Row> overLnDa = processedDF.filter("check_port = std_mod_line_cnt + 1 or check_port > std_mod_line_cnt + 2");

        Dataset<Row> lackLn2doc = lackLn2doc(lackLnDa);

        Dataset<Row> overLn2doc = overLn2doc(overLnDa);

        Dataset<Row> linkLn2FullLn = Algorithm.linkLnEstimate(linkLnDa);

        Dataset<Row> fullLnTtl = fullLnDa.select(col("sim_id"), col("mc_id"), col("dt"),
                col("first_draw_time"), col("line_no"), col("lead_x"), col("lead_y"),
                col("pad_x"), col("pad_y"), col("check_port"), col("pieces_index"),
                col("sub_mc_id"), col("cnt"), col("wire_len"))
                .union(linkLn2FullLn)
                .sort(col("sim_id"), col("mc_id"), col("first_draw_time"), col("pieces_index"), col("line_no"));

        // 自排序算法
        /* Dataset<Row> normalizeUnitDF = coordinateNormalization(fullLnTtl); */

        // normalizeUnitDF.show(showCount);

        /* Dataset<Row> normalizeDF = generateLineNmb(normalizeUnitDF); */

        // normalizeDF.show(showCount);

        Dataset<Row> fullLnDiff = diff(fullLnTtl);

        Dataset<Row> fullLnSta = fullLnMkSta(fullLnDiff, stdModels);

        Dataset<Row> fullLn2doc = fullLn2doc(fullLnSta);

        Dataset<Row> ttlLn = DataProcess.unionDataFrame(new ArrayList<>(Arrays.asList(noProgDF, lackLn2doc, fullLn2doc, overLn2doc))).withColumn("program_name", lit("wb_comparison"));

        Dataset<Row> ctrlInfoDF = ttlLn.select(col("sim_id"), col("program_name"), col("dt"), col("code"), col("description"));

        boolean flag = new Persist().doPost(ctrlInfoDF);
        if (!flag) {
            log.error("写入redis部分或者全部出错！");
        }

        Properties postgresProp = new Properties();
        postgresProp.put("user", postgresUser);
        postgresProp.put("password", postgresPwd);

        /* --比对结果入postgres */
        upsertPostgres(ctrlInfoDF, postgresUrl, postgresProp);
        log.warn(">>>> upsert postgresql result done.");

        // update job run dt
        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobRunDt", JSONObject.parseObject("{'wb_comparison_result':'" + nextRunDt + "'}"));
        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobRunDt", JSONObject.parseObject("{'wb_comparison_raw':'" + maxDtOfRawDF + "'}"));
        HttpConnectUtils.post("http://10.170.6.40:32767/job/api/updateJobStat", JSONObject.parseObject("{'wb_comparison_result':'0'}"));
        log.warn(">>>> update job run dt done.");

        Dataset<Row> appendDF = comparisonDF.select(col("sim_id"), col("mc_id"), col("dt"), col("line_no"), col("lead_x"), col("lead_y"), col("pad_x"), col("pad_y"), col("check_port"),
                col("pieces_index")).where(String.format("`dt` > '%s'", druidBeginDate)).withColumn("load_time", lit(DatetimeBuilder("yyyy-MM-dd HH:mm:ss")));
        /* --坐标明细入库 */
        sav2Doris(appendDF, dorisDriver, dorisUrl, dorisUser, dorisPwd, coordinationDetailTb);
        log.warn(">>>> insert to doris raw data done.");

        /* --比对明细入库 */
        sav2Doris(ttlLn.filter(String.format("dt > '%s'", druidBeginDate)), dorisDriver, dorisUrl, dorisUser, dorisPwd, comparisonDetailTb);
        log.warn(">>>> insert to doris comparison detail data done.");

        comparisonDF.unpersist();
        processedDF.unpersist();

        /* --更新比对时间 */
        updateJobRunDt("wb_comparison_result", nextRunDt);
        updateJobRunDt("wb_comparison_raw", maxDtOfRawDF);
        log.warn("update job run datetime on database done.");

        long timeEnd = System.currentTimeMillis();
        log.warn(">>>> job run spend {} s.", Math.round((timeEnd - timeStart) / ComparisonInfo.SECONDS_UNIT.getFlt()));
    }
}
