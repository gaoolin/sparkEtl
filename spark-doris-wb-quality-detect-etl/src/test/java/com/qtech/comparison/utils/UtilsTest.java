package com.qtech.comparison.utils;

import com.qtech.comparison.ibatis.mapper.JobRunInfoMapper;
import com.qtech.comparison.ibatis.pojo.JobRunInfo;
import org.junit.Test;

import java.io.IOException;

import static com.qtech.comparison.ibatis.config.MapperInstance.getMapper;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/14 10:40:10
 * desc   :
 */


public class UtilsTest {

    private static JobRunInfoMapper jobRunInfoMapper = null;

    static {
        try {
            jobRunInfoMapper = getMapper(ComparisonInfo.DATASOURCE_DRUID_DORIS.getStr(), JobRunInfoMapper.class);
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getJobRunDt() {
        JobRunInfo jobRunInfo = jobRunInfoMapper.getJobRunDt("wb_comparison_result");
        System.out.println(jobRunInfo.toString());
        System.out.println(jobRunInfo.getPreRunTime());
    }
}
