package com.qtech.comparison.ibatis.mapper;


import com.qtech.comparison.ibatis.pojo.JobRunInfo;
import com.qtech.comparison.ibatis.pojo.WbComparisonStdModel;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhilin.gao
 * @since 2022-06-10
 */
public interface StdModelsMapper {

	List<WbComparisonStdModel> getAll();

	public JobRunInfo getJobRunDt(@Param("jobName") String jobName);

	public JobRunInfo getJobRunStatus(@Param("jobName") String jobName);

	Integer updateJobRunStatus(@Param("jobName") String jobName, @Param("status") Integer status);

	public int updateJobRunDt(@Param("jobName") String jobName, @Param("preRunTime") String preRunTime);
}
