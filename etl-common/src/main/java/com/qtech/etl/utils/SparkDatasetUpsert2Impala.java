package com.qtech.etl.utils;

import com.qtech.etl.exception.biz.wire.ImpalaUpsertUnknownDataTypeException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/18 13:39:03
 * @description : Spark Dataset upsert into impala
 */


public class SparkDatasetUpsert2Impala implements Serializable {

    private long count = 0;
    private Dataset<Row> df = null;
    private Map<String, String> args = new HashMap<>();
    private Boolean allFieldString;

    public SparkDatasetUpsert2Impala() {
    }

    /**
     * @param df   Spark Dataset格式数据
     * @param args 需要JDBC连接的driver、url、username、password、table
     * @param allFieldString 是否把 Dataset 的数据所有字段转换成String插入数据库
     * @return SparkDatasetUpsert2Impala对象
     * @description 构造方法
     */
    public SparkDatasetUpsert2Impala(Dataset<Row> df, Map<String, String> args, Boolean allFieldString) {
        this.df = df;
        this.args = args;
        this.allFieldString = allFieldString;
    }

    public SparkDatasetUpsert2Impala(Dataset<Row> df, Map<String, String> args) {
        this.df = df;
        this.args = args;
        this.allFieldString = true;
    }

    public long doUpsert() {
        // 用户信息和url
        String driver = this.args.get("driver");
        String url = this.args.get("url");
        String username = this.args.get("username");
        String password = this.args.get("password");
        String tableName = this.args.get("table");
        int fieldSize = this.df.schema().size();
        String[] fieldNames = this.df.schema().fieldNames();
        StructField[] fields = this.df.schema().fields();
        List<DataType> dataType = new ArrayList<>();
        for (StructField field : fields) {
            if (!allFieldString) {
                dataType.add(field.dataType());
            } else {
                dataType.add(DataTypes.StringType);
            }
        }
        String upsertSql = String.format("upsert into %s(%s) values(%s)", tableName, join(fieldNames, ","), join(Collections.nCopies(fieldSize, "?"), ","));

        this.df.foreachPartition(iter -> {
            // 1.加载驱动
            Class.forName(driver);
            // 3.连接成功，数据库对象 Connection
            Connection connection = DriverManager.getConnection(url, username, password);
            // 关闭事务的自动提交
            connection.setAutoCommit(false);
            // 4.执行SQL对象Statement，执行SQL的对象
            Statement statement = connection.createStatement();

            PreparedStatement pstmt = connection.prepareStatement(upsertSql);

            while (iter.hasNext()) {
                Row row = iter.next();
                for (int i = 0; i < fieldSize; i++) {
                    if (DataTypes.StringType.sameType(dataType.get(i))) {
                        pstmt.setString(i + 1, row.getAs(i) == null ? null : row.getAs(i).toString());
                    } else if (DataTypes.IntegerType.sameType(dataType.get(i))) {
                        pstmt.setInt(i + 1, row.getAs(i));
                    } else if (DataTypes.BooleanType.sameType(dataType.get(i))) {
                        pstmt.setBoolean(i + 1, row.getAs(i));
                    } else if (DataTypes.FloatType.sameType(dataType.get(i))) {
                        pstmt.setFloat(i + i, row.getAs(i));
                    } else if (DataTypes.TimestampType.sameType(dataType.get(i))) {
                        pstmt.setTimestamp(i + 1, row.getAs(i));
                    } else {
                        throw new ImpalaUpsertUnknownDataTypeException();
                    }
                }
                int i = pstmt.executeUpdate();
                count = count + i;
            }
            // impala jdbc 不支持 pstmt.executeBatch()。异常：java.sql.SQLException: [Cloudera][ImpalaJDBCDriver](500057) Multi-batch parameter values not supported for this query type.
            connection.commit();
            connection.setAutoCommit(true);
            // 6.释放连接
            statement.close();
            connection.close();
        });
        return count;
    }
}
