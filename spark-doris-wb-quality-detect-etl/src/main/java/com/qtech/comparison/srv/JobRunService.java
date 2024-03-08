package com.qtech.comparison.srv;

import com.qtech.comparison.utils.Utils;
import com.qtech.etl.utils.HttpConnectUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/03/08 13:38:26
 * desc   :
 */

@Slf4j
public class JobRunService {

    private static String resRunDt;
    private static String rawRunDt;
    private static final String stat;
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        stat = jobRunStat();
        resRunDt = jobRunDt("wb_comparison_result");
        rawRunDt = jobRunDt("wb_comparison_raw");
    }

    private static String jobRunDt(String jobName) {
        String redisUrl = String.format("http://10.170.6.40:32767/job/api/getJobRunDt/%s", jobName);
        return HttpConnectUtils.get(redisUrl);
    }

    private static String jobRunStat() {
        return HttpConnectUtils.get("http://10.170.6.40:32767/job/api/getJobStat/wb_comparison_result");
    }

    public static Boolean checkJobRunCondition() {
        if ("0".equals(stat)) {
            return true;
        } else if ("1".equals(stat)) {
            if (Strings.isNullOrEmpty(resRunDt)) {
                String preRunDt = Utils.getJobRunDt("wb_comparison_result");
                if (Strings.isNullOrEmpty(preRunDt)) {
                    resRunDt = LocalDateTime.now().minusMinutes(15).format(dateTimeFormatter);
                } else {
                    Duration duration = Duration.between(LocalDateTime.parse(resRunDt, dateTimeFormatter), LocalDateTime.now());
                    if (duration.toMinutes() < 15) {
                        log.warn("已有打线图作业正在运行，本次作业将退出！");
                        return false;
                    }
                }
            } else {
                Duration duration = Duration.between(LocalDateTime.parse(resRunDt, dateTimeFormatter), LocalDateTime.now());
                if (duration.toMinutes() < 15) {
                    log.warn("已有打线图作业正在运行，本次作业将退出！");
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public static Map<String, String> getFinalJobRunDt() {
        HashMap<String, String> jobDtMap = new HashMap<>();

        if (Strings.isNullOrEmpty(resRunDt)) {
            resRunDt = Utils.getJobRunDt("wb_comparison_result");
            if (Strings.isNullOrEmpty(resRunDt)) {
                resRunDt = LocalDateTime.now().minusMinutes(15).format(dateTimeFormatter);
            }
        }

        if (Strings.isNullOrEmpty(rawRunDt)) {
            rawRunDt = Utils.getJobRunDt("wb_comparison_raw");
            if (Strings.isNullOrEmpty(rawRunDt)) {
                resRunDt = LocalDateTime.now().format(dateTimeFormatter);
            }
        }

        jobDtMap.put("wb_comparison_result", resRunDt);
        jobDtMap.put("wb_comparison_raw", rawRunDt);

        return jobDtMap;
    }
}
