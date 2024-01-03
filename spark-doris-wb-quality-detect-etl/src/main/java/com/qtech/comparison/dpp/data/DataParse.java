package com.qtech.comparison.dpp.data;

import com.qtech.comparison.exception.NoDataFetchedFromDruidException;
import com.qtech.comparison.utils.ComparisonInfo;
import com.qtech.etl.utils.PropertiesManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2022/1/4 11:41
 */
@Slf4j
public class DataParse {

    public static Dataset<Row> druidParse(SparkSession ss, String druidBeginDate) {

        /*PropertiesManager.loadProp("WbComparison.properties");*/
        PropertiesManager pm = PropertiesManager.getInstance();

        String querySql = String.format(ComparisonInfo.DRUID_RAW_DATA_SQL.getStr(), druidBeginDate);
//        String querySql = ComparisonInfo.DRUID_RAW_DATA_SQL_TEST.getStr();

        String driver = pm.getString("jdbc.druid.driver");
        String url = pm.getString("jdbc.druid.url");

        Dataset<Row> druidDF = ss.read().format("jdbc")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", "(" + querySql + ") tmp")
                .option("user", "")
                .option("password", "")
                .load();

        if (druidDF.count() == 0) {
            log.error(">>> Druid has no data at the begin time: {}", druidBeginDate);
            ss.close();
            throw new NoDataFetchedFromDruidException();
            /*System.exit(-1);*/
        }
        druidDF.createOrReplaceTempView("t_dev_attrs");

        return ss.sql(ComparisonInfo.DRUID_TRANSFORM_SQL.getStr()).filter("line_no > 0");
    }
}
