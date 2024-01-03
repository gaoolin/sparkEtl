package com.qtech.etl.utils;

import com.qtech.etl.exception.MissingParametersForDoInsertException;
import com.qtech.etl.exception.biz.UnknownDataTypeException;
import com.qtech.etl.exception.biz.wire.DorisInsertUnknownDataTypeException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/08 11:45:06
 * desc   :  Spark Dataset insert into DB
 */


public class SparkDataset2DbUtil {

    private static long count = 0;

    private static long batchIndex = 0;

    private static final int batchSize = 2000;


    /**
     * @param
     * @return
     * @description Spark Dataset数据集插入数据 prop中必须配置至少五个属性：driver, url, table, user, password
     */
    public static long doInsert(Dataset<Row> df, Properties prop, Integer batchSize, Boolean allFieldString) {
        // 数据库信息
        String driver = prop.getProperty("driver");
        String url = prop.getProperty("url");
        String tableName = prop.getProperty("table");

        if (driver == null || url == null || tableName == null) {
            throw new MissingParametersForDoInsertException();
        }

        int fieldSize = df.schema().size();
        String[] fieldNames = df.schema().fieldNames();
        StructField[] fields = df.schema().fields();
        ArrayList<DataType> dataType = new ArrayList<>();

        for (StructField field : fields) {
            if (!allFieldString) {
                dataType.add(field.dataType());
            } else {
                dataType.add(DataTypes.StringType);
            }
        }

        String insertSql = String.format("insert into %s(%s) values(%s)", tableName, join(fieldNames, ","), join(Collections.nCopies(fieldSize, "?"), ","));

        df.foreachPartition(iter -> {

            // 1.加载驱动
            Class.forName(driver);
            // 2.数据库对象 connection
            Connection conn = DriverManager.getConnection(url, prop);
            // 3.关闭事务的自动提交
            conn.setAutoCommit(false);
            // 4.执行SQL对象statement
            Statement statement = conn.createStatement();

            PreparedStatement pstmt = conn.prepareStatement(insertSql);
            try {
                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (int i = 0; i < fieldSize; i++) {
                        if (row.getAs(i) == null) {
                            if (DataTypes.StringType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.VARCHAR);
                            } else if (DataTypes.IntegerType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.INTEGER);
                            } else if (DataTypes.DateType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.DATE);
                            } else if (DataTypes.LongType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.BIGINT);
                            } else if (DataTypes.FloatType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.FLOAT);
                            } else if (DataTypes.TimestampType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.TIMESTAMP_WITH_TIMEZONE);
                            } else if (DataTypes.BooleanType.sameType(dataType.get(i))) {
                                pstmt.setNull(i + 1, Types.BOOLEAN);
                            } else {
                                throw new DorisInsertUnknownDataTypeException();
                            }
                        } else {
                            if (DataTypes.StringType.sameType(dataType.get(i))) {
                                pstmt.setString(i + 1, row.getAs(i).toString());
                            } else if (DataTypes.IntegerType.sameType(dataType.get(i))) {
                                pstmt.setInt(i + 1, row.getAs(i));
                            } else if (DataTypes.DateType.sameType(dataType.get(i))) {
                                pstmt.setDate(i + 1, row.getAs(i));
                            } else if (DataTypes.LongType.sameType(dataType.get(i))) {
                                pstmt.setLong(i + 1, row.getAs(i));
                            } else if (DataTypes.FloatType.sameType(dataType.get(i))) {
                                pstmt.setFloat(i + 1, row.getAs(i));
                            } else if (DataTypes.TimestampType.sameType(dataType.get(i))) {
                                pstmt.setTimestamp(i + 1, row.getAs(i));
                            } else if (DataTypes.BooleanType.sameType(dataType.get(i))) {
                                pstmt.setBoolean(i + 1, row.getAs(i));
                            } else {
                                throw new UnknownDataTypeException();
                            }
                        }
                    }
                    // 5.加入批次
                    pstmt.addBatch();
                    batchIndex += 1;

                    // 6.控制提交的数量,
                    // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！
                    if (batchIndex % batchSize == 0) {
                        pstmt.executeBatch();
                        pstmt.clearBatch();
                    }
                    count += 1;
                }
                // 7.提交不足1000条SQL的批次
                pstmt.executeBatch();
                pstmt.clearBatch();
                // 8.手动提交
                conn.commit();
            } catch (SQLException | DorisInsertUnknownDataTypeException | UnknownDataTypeException throwables) {
                throwables.printStackTrace();
            } finally {
                // 9.恢复 默认开启事务设置
                conn.setAutoCommit(true);
                // 10.释放连接
                statement.close();
                conn.close();
            }
        });
        return count;
    }

    public static long doInsert(Dataset<Row> df, Properties prop, Integer batchSize) {
        return doInsert(df, prop, batchSize, false);
    }

    public static long doInsert(Dataset<Row> df, Properties prop, Boolean allFieldString) {
        return doInsert(df, prop, batchSize, allFieldString);
    }

    public static long doInsert(Dataset<Row> df, Properties prop) {
        return doInsert(df, prop, batchSize, false);
    }
}
