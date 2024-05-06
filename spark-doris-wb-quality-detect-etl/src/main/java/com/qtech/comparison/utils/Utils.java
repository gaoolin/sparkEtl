package com.qtech.comparison.utils;

import com.qtech.comparison.ibatis.mapper.JobRunInfoMapper;
import com.qtech.comparison.ibatis.mapper.StdModelsMapper;
import com.qtech.comparison.ibatis.pojo.JobRunInfo;
import com.qtech.comparison.ibatis.pojo.WbComparisonStdModel;
import com.qtech.etl.config.HadoopConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import static com.qtech.comparison.ibatis.config.MapperInstance.getMapper;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/12 10:46:44
 * desc   :  工具类
 */
public class Utils {

    private static JobRunInfoMapper jobRunInfoMapper = null;
    private static StdModelsMapper stdModelsMapper = null;

    static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        try {
            // jobRunInfoMapper = getMapper(ComparisonInfo.DATASOURCE_DRUID_DORIS.getStr(), JobRunInfoMapper.class);
            jobRunInfoMapper = getMapper(ComparisonInfo.DATASOURCE_DRUID_POSTGRES.getStr(), JobRunInfoMapper.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static {
        try {
            stdModelsMapper = getMapper(ComparisonInfo.DATASOURCE_DRUID_POSTGRES.getStr(), StdModelsMapper.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getJobRunDt(String jobName) {
        JobRunInfo jobRunDt = jobRunInfoMapper.getJobRunDt(jobName);
        return jobRunDt.getPreRunTime() ;
    }

    public static int updateJobRunDt(String jobName, String preRunTime) {
        return jobRunInfoMapper.updateJobRunDt(jobName, Timestamp.valueOf(preRunTime));
    }

    public static Integer getJobStat(String jobName) {
        return jobRunInfoMapper.getJobRunStatus(jobName).getStatus();
    }

    public static Integer updateJobStat(String jobName, Integer status) {
        return jobRunInfoMapper.updateJobRunStatus(jobName, status);
    }

    public static List<WbComparisonStdModel> getStdModels() {
        return stdModelsMapper.getAll();
    }

    public static Integer getJobRunStatus(String jobName) {
        return jobRunInfoMapper.getJobRunStatus(jobName).getStatus();
    }

    public static Integer updateJobRunStatus(String jobName, Integer status) {
        return jobRunInfoMapper.updateJobRunStatus(jobName, status);
    }

    public static String DatetimeBuilder(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar cal = Calendar.getInstance();

        return sdf.format(cal.getTime());
    }

    public static String getMaxDtAndOffset(Dataset<Row> rawDF, int lagMinutes, String dtField) {
        Dataset<Row> filterDF = rawDF.filter(dtField + " is not null");
        String dtStr = String.valueOf(filterDF.select(max(col(dtField)).alias(dtField)).head().get(0));
        String maxDt = dtStr.substring(0, 19);
        LocalDateTime toDt = LocalDateTime.parse(maxDt, dateTimeFormatter);
        LocalDateTime nextJobDt = toDt.minusMinutes(lagMinutes);
        DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        return dtf.format(nextJobDt).replace("T", " ");
    }

    public static String getMaxDtAndOffset(Dataset<Row> rawDF, Integer lagMinutes) {
        String timeField = "dt";

        return getMaxDtAndOffset(rawDF, lagMinutes, timeField);
    }

    public static String getMaxDtAndOffset(Dataset<Row> rawDF) {
        int lagMinutes = 0;

        return getMaxDtAndOffset(rawDF, lagMinutes);
    }

    public static String offsetTime(String beginDt, int lagMinutes) {
        LocalDateTime toDt = LocalDateTime.parse(beginDt, dateTimeFormatter);
        LocalDateTime offsetDt = toDt.minusMinutes(lagMinutes);
        DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        return dtf.format(offsetDt).replace("T", " ");
    }


    public static void write2File(String dt, String path) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 0);

        try {
            FileSystem fileSystem = FileSystem.get(HadoopConfig.setHadoopConf(new Configuration()));

            FSDataOutputStream outputStream = fileSystem.create(new Path(path));

            outputStream.write(dt.getBytes(StandardCharsets.UTF_8));

            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String readFromFile(String readPath, JavaSparkContext jsc) {
        String lines = jsc.textFile(readPath).rdd().first();
        System.out.println(jsc.textFile(readPath).rdd());

        return lines;
    }

    public static void updateOracleEqCtrl(Dataset<Row> df, String url, String user, String password) throws SQLException {

        Connection conn = DriverManager.getConnection(url, user, password);

        PreparedStatement updatePstm = conn.prepareStatement("UPDATE equipment_control_info SET dt = ?," +
                " code = ?, description =? WHERE sim_id = ? AND program_name = 'WB_AD'");

        PreparedStatement insertPstm = conn.prepareStatement("INSERT INTO equipment_control_info(sim_id, " +
                "program_name, dt, code, description) VALUES(?, ?, ?, ?, ?)");

        try {
            for (Row row : df.collectAsList()) {
                updatePstm.setString(1, row.getString(2));
                updatePstm.setString(2, String.valueOf(row.getInt(3)));
                updatePstm.setString(3, row.getString(4));
                updatePstm.setString(4, row.getString(0));
                updatePstm.execute();
                if (updatePstm.getUpdateCount() == 0) {
                    insertPstm.setString(1, row.getString(0));
                    insertPstm.setString(2, "WB_AD");
                    insertPstm.setString(3, row.getString(2));
                    insertPstm.setString(4, String.valueOf(row.getInt(3)));
                    insertPstm.setString(5, row.getString(4));
                    insertPstm.execute();
                }
            }
        } finally {
            conn.close();
            updatePstm.close();
            insertPstm.close();
        }
    }

    /**
     * @param
     * @return
     * @description 为避免同一个pieces index、check port抓取到两个或者多个同一机型产品的坐标数据导致线号排序出问题， 进而影响判定结果的情形，规定抓取数据的时间区间不超过45分钟，
     * 如果时间超出45分钟，则以现在的时间往前推45分钟为准。
     */
    public static String judgeRunDt(String preRunDt, String nowDt, int intervalMinutes) {
        LocalDateTime pre = LocalDateTime.parse(preRunDt, dateTimeFormatter);
        LocalDateTime now = LocalDateTime.parse(nowDt, dateTimeFormatter);
        LocalDateTime judge = now.minusMinutes(intervalMinutes);
        DateTimeFormatter dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        return pre.isAfter(judge) ? dtf.format(pre).replace("T", " ") : dtf.format(judge).replace("T", " ");
    }

    public static String judgeRunDt(String preRunDt, String nowDt) {
        return judgeRunDt(preRunDt, nowDt, 45);
    }

    public static Dataset<Row> getStdModels(SparkSession ss, String postgresDriver, String postgresUrl, String postgresUser, String postgresPwd) {
        return ss.read().format("jdbc")
                .option("driver", postgresDriver)
                .option("url", postgresUrl)
                .option("dbtable", "(" + ComparisonInfo.STD_MOD_SQL.getStr() + ") tmp")
                .option("user", postgresUser)
                .option("password", postgresPwd)
                .load();
    }

    public static void sav2Doris(Dataset<Row> df, String dorisDriver, String dorisUrl, String dorisUser, String dorisPwd, String dorisTb) {
        df.write().format("jdbc")
                .mode(SaveMode.Append)
                .option("driver", dorisDriver)
                .option("url", dorisUrl)
                .option("user", dorisUser)
                .option("password", dorisPwd)
                .option("isolationLevel", "NONE")
                .option("dbtable", dorisTb)
                .option("batchsize", "5000")
                .save();
    }

    public static void upsertPostgres(Dataset<Row> df, String url, Properties prop) throws SQLException, ClassNotFoundException {

        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url, prop);
        PreparedStatement upsertPstm = conn.prepareStatement(
                "insert into qtech_wb.public.eq_control_info (sim_id, program_name, dt, code, description) " +
                        "values(?, ?, ?, ?, ?) " +
                        "ON CONFLICT ON CONSTRAINT eq_control_info_unique_key " +
                        "DO " +
                        "UPDATE SET dt = ?, code = ?, description = ?;");

        try {
            for (Row row : df.collectAsList()) {
                upsertPstm.setString(1, row.getString(0));
                upsertPstm.setString(2, row.getString(1));
                upsertPstm.setTimestamp(3, Timestamp.valueOf(row.getString(2)));
                upsertPstm.setInt(4, row.getInt(3));
                upsertPstm.setString(5, row.getString(4));
                upsertPstm.setTimestamp(6, Timestamp.valueOf(row.getString(2)));
                upsertPstm.setInt(7, row.getInt(3));
                upsertPstm.setString(8, row.getString(4));
                upsertPstm.executeUpdate();
            }
        } finally {
            conn.close();
            upsertPstm.close();
        }
    }

    public static Dataset<Row> getNeedFilterMcId(SparkSession ss, String postgresDriver, String postgresUrl, String postgresUser, String postgresPwd) {
        return ss.read().format("jdbc")
                .option("driver", postgresDriver)
                .option("url", postgresUrl)
                .option("dbtable", "(" + ComparisonInfo.NEED_FILTER_MCID_SQL.getStr() + ") tmp")
                .option("user", postgresUser)
                .option("password", postgresPwd)
                .load();
    }
}
