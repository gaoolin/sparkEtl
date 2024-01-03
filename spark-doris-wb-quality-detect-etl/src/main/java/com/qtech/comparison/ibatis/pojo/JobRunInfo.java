package com.qtech.comparison.ibatis.pojo;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author zhilin.gao
 * @since 2022-06-09
 */

public class JobRunInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String jobName;

    private String preRunTime;

    private Integer status;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getPreRunTime() {
        return preRunTime;
    }

    public void setPreRunTime(String preRunTime) {
        this.preRunTime = preRunTime;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "JobRunDt{" +
                "jobName='" + jobName + '\'' +
                ", preRunTime='" + preRunTime + '\'' +
                ", status=" + status +
                '}';
    }
}
