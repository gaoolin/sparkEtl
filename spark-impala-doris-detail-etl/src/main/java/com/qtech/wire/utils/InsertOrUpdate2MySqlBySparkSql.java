package com.qtech.wire.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.*;
import java.util.Properties;

/**
 * @author : gaozhilin
 * @project : qtech-kudu-wire-usage-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/10 11:40:15
 * @description :
 */


public class InsertOrUpdate2MySqlBySparkSql implements Serializable {

    private Properties args = new Properties();
    private Dataset<Row> df = null;

    public InsertOrUpdate2MySqlBySparkSql() {
    }

    public InsertOrUpdate2MySqlBySparkSql(Properties args, Dataset<Row> df) {
        this.args = args;
        this.df = df;
    }

    public void doInsertOrUpdate() {
        /*
         * ** 每个分区创建一个mysql连接，能大大降低连接数
         * 此步骤可能会发生异常：Caused by: java.io.NotSerializableException: com.mysql.jdbc.JDBC4PreparedStatement
         * 原因是：prep是一个PrepareStatement对象，这个对象无法序列化，而传入map中的对象是需要分布式传送到各个节点上，传送前先序列化，到达相应机器上后再反序列化，PrepareStatement是个Java类，如果一个java类想(反)序列化，必须实现Serialize接口，PrepareStatement并没有实现这个接口，对象prep在driver端，collect后的数据也在driver端，就不需prep序列化传到各个节点了。
         * 但这样其实会有collect的性能问题，
         * 解决方案：使用foreachPartition/mapPartition在每一个分区内维持一个mysql连接进行插入
         */

        this.df.foreachPartition(iter -> {
            String driver = this.args.getProperty("driver");
            // 1.加载驱动
            Class.forName(driver);
            // 2.用户信息和url
            String url = this.args.getProperty("url");
            String user = this.args.getProperty("user");
            String password = this.args.getProperty("password");
            String charset = this.args.getProperty("charset");
            // 3.连接成功，数据库对象 Connection
            Connection connection = DriverManager.getConnection(url, args);
            // 关闭事务的自动提交
            connection.setAutoCommit(false);
            // 4.执行SQL对象Statement，执行SQL的对象
            Statement statement = connection.createStatement();
            PreparedStatement prep = connection.prepareStatement("update public.job_run_status set pre_run_time = ? where job_name = 'wb_wire_usage'");
            while (iter.hasNext()) {
                Row row = iter.next();
                String dt = row.getAs("pre_run_time");
                // 5.执行SQL的对象去执行SQL，返回结果集
                prep.setTimestamp(1, Timestamp.valueOf(dt));
                prep.addBatch();
            }
            prep.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            // 6.释放连接
            statement.close();
            connection.close();
        });
    }
}
