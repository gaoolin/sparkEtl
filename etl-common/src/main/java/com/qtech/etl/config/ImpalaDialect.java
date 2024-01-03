package com.qtech.etl.config;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.sql.Types;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/17 11:35:56
 * @description : TODO
 */


class SparkJdbcDialectConfig extends JdbcDialect {

    private static final Logger logger = LoggerFactory.getLogger(SparkJdbcDialectConfig.class);

    @Override
    public boolean canHandle(String url) {
        logger.info(">>>>> 使用{}自定义方言", url.split(":")[1]);
        return true;
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        return super.getCatalystType(sqlType, typeName, size, md);
    }

    @Override
    public String quoteIdentifier(String colName) {
        return colName.replace("\"", "");
        /* 或者不做处理 */
        /* return colName; */
    }

}

public class ImpalaDialect extends JdbcDialect {

    @Override
    public boolean canHandle(String url) {
         return url.startsWith("jdbc:impala") || url.contains("impala");
    }

    @Override
    public String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        return super.getCatalystType(sqlType, typeName, size, md);
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (dt instanceof StringType) {
            return Option.apply(new JdbcType("String", Types.VARCHAR));
        }
        return super.getJDBCType(dt);
    }
}
