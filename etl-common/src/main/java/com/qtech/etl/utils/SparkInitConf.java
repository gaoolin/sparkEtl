package com.qtech.etl.utils;

import com.qtech.etl.exception.biz.comparison.SparkDppException;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/01/02 10:23:59
 * desc   :  用于初始胡SparkSession的配置
 */


public class SparkInitConf {

    private static Map<String, String> getHadoopConfigurations() {

        HashMap<String, String> hadoopConf = new HashMap<>();
        hadoopConf.put("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.put("fs.hdfs.impl.disable.cache", "true");
        hadoopConf.put("fs.defaultFS", "hdfs://cluster");
        hadoopConf.put("dfs.nameservices", "cluster");
        hadoopConf.put("dfs.ha.namenodes.cluster", "nn1,nn2");
        hadoopConf.put("dfs.namenode.rpc-address.cluster.nn1", "k8s-nod05:8020");
        hadoopConf.put("dfs.namenode.rpc-address.cluster.nn2", "k8s-nod06:8020");
        hadoopConf.put("dfs.client.failover.proxy.provider.cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return hadoopConf;
    }

    public static SparkConf initSparkConfigs() throws SparkDppException {
        SparkConf conf = new SparkConf();
        Map<String, String> configs = getHadoopConfigurations();
        if (configs.size() == 0) {
            throw new SparkDppException("Spark 配置初始化出错，创建上下文的配置为空，请检查！");
            // System.exit(-1);  // 退出jvm，结束整个程序
        }
        /*优点快、小，缺点需要注册不支持所有的Serializable类型、且需要用户注册要进行序列化的类class，shuffle的数据量较大或者较为频繁时建议使用*/
        //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
            conf.set("spark.hadoop." + entry.getValue(), entry.getValue());
        }
        return conf;
    }
}
