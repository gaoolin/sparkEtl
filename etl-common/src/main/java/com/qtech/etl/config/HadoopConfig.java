package com.qtech.etl.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/17 11:35:29
 * @description : TODO
 */


public class HadoopConfig {

    public static Configuration setHadoopConf(Configuration hadoopConf) {

        System.setProperty("HADOOP_USER_NAME", "zcgx");

        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.set("fs.hdfs.impl.disable.cache", "true");
        hadoopConf.set("fs.defaultFS", "hdfs://nameservice");
        hadoopConf.set("dfs.nameservices", "nameservice");
        hadoopConf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02");
        hadoopConf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020");
        hadoopConf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020");
        hadoopConf.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        return hadoopConf;
    }


    public static void setSparkContextHadoopConf(JavaSparkContext jsc) {

        setHadoopConf(jsc.hadoopConfiguration());
    }
}
