package com.qtech.comparison.ibatis.mapper;


import com.qtech.comparison.ibatis.pojo.JobRunInfo;
import org.apache.ibatis.annotations.Param;

import java.sql.Timestamp;

/**
 * <p>
 * Mapper 接口
 * </p>
 *
 * @author zhilin.gao
 * @since 2022-06-09
 */

public interface JobRunInfoMapper {

    public JobRunInfo getJobRunDt(@Param("jobName") String jobName);

    public int updateJobRunDt(@Param("jobName") String jobName, @Param("preRunTime") Timestamp preRunTime);

    public JobRunInfo getJobRunStatus(@Param("jobName") String jobName);

    public int updateJobRunStatus(@Param("jobName") String jobName, @Param("status") Integer status);
}
