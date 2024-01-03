package com.qtech.etl.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import javax.validation.constraints.NotNull;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/17 11:32:12
 * @description : TODO
 */


public class SparkConfig {

    private String appName;
    private String master;

    public SparkConfig() {

    }

    public SparkConfig(String appName) {
        this.appName = appName;
    }

    public SparkConfig(String appName, String master) {
        this.appName = appName;
        this.master = master;

    }

    @NotNull
    public SparkConf initSparkConf() {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(this.appName);

        if (StringUtils.isNotBlank(this.master) && StringUtils.isNotEmpty(this.master)) {
            sparkConf.setMaster(this.master);;
        }

        // sparkConf.set("spark.sql.crossJoin.enabled", "true");

        return sparkConf;
    }
}
