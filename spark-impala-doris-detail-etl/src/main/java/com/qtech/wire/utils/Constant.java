package com.qtech.wire.utils;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/26 11:02:23
 * @description :
 */


public class Constant {

    public static final String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    public static final String IMPALA_JDBC_URL = "jdbc:impala://10.170.3.15:21050/ods_machine_extract;UseSasl=0;" +
            "AuthMech=3;UID=qtkj;PWD=;characterEncoding=utf-8";

    public static final String KUDU_MASTER = "10.170.3.11:7051,10.170.3.12:7051,10.170.3.13:7051";

    public static final String COMBINE_AREO_WIRE_USAGE_SQL_PREFIX = "select device_id,\n" +
            "       device_m_id,\n" +
            "       prod_type,\n" +
            "       create_date,\n" +
            "       factory_name,\n" +
            "       workshop,\n" +
            "       machine_no,\n" +
            "       wire_cnt,\n" +
            "       prod_cnt,\n" +
            "       wire_usage,\n" +
            "       yield,\n" +
            "       wire_size,\n" +
            "       device_type,\n" +
            "       biz_date,\n" +
            "       time_period,\n" +
            "       upload_time\n" +
            "from (select tb.device_id,\n" +
            "             tb.device_m_id,\n" +
            "             prod_type,\n" +
            "             create_date,\n" +
            "             factory_name,\n" +
            "             workshop_code                                                                                  as workshop,\n" +
            "             machine_no,\n" +
            "             wire_cnt,\n" +
            "             prod_cnt,\n" +
            "             (wire_cnt - lag(wire_cnt, 1) over (partition by tb.device_id, prod_type order by create_date)) as wire_usage,\n" +
            "             (prod_cnt - lag(prod_cnt, 1) over (partition by tb.device_id, prod_type order by create_date)) as yield,\n" +
            "             wire_size,\n" +
            "             ta.device_type,\n" +
            "             biz_date,\n" +
            "             time_period,\n" +
            "             upload_time\n" +
            "      from (select device_id,\n" +
            "                   lotname                                                                   as prod_type,\n" +
            "                   create_date,\n" +
            "                   cast(split_part(wire_turn_count, \".\", 1) as bigint)                       as wire_cnt,\n" +
            "                   cast(split_part(unitbondtotal, \".\", 1) as bigint)                         as prod_cnt,\n" +
            "                   Wire_Size_EFO_Parameter                                                   as wire_size,\n" +
            "                   0                                                                    as device_type,\n" +
            "                   substr(cast(hours_sub(create_date, 8) as string), 1, 10)                  as biz_date,\n" +
            "                   if(cast(substr(create_Date, 12, 2) as float) < 9,\n" +
            "                      concat(substr(create_Date, 12, 2), '~',\n" +
            "                             concat('0', cast(cast(substr(create_Date, 12, 2) as float) + 1 as string))),\n" +
            "                      concat(substr(create_Date, 12, 2), '~',\n" +
            "                             cast(cast(substr(create_Date, 12, 2) as float) + 1 as string))) as time_period,\n" +
            "                   now()                                                                     as upload_time\n" +
            "            from ods_machine_extract.wb_areo\n" +
            "            where create_date > '";

    public static final String COMBINE_XTREME_WIRE_USAGE_SQL_PREFIX = "' and (wire_turn_count <> \"0\"\n" +
            "                or unitbondtotal <> \"0\")\n" +
            "              and lotname <> \"\"\n" +
            "            union all\n" +
            "            select device_id,\n" +
            "                   lotname                                                                   as prod_type,\n" +
            "                   create_date,\n" +
            "                   cast(split_part(wire_turn_count, \".\", 1) as bigint)                       as wire_cnt,\n" +
            "                   cast(split_part(unitbondtotal, \".\", 1) as bigint)                         as prod_cnt,\n" +
            "                   Wire_Size_EFO_Parameter                                                   as wire_size,\n" +
            "                   1                                                                 as device_type,\n" +
            "                   substr(cast(hours_sub(create_date, 8) as string), 1, 10)                  as biz_date,\n" +
            "                   if(cast(substr(create_Date, 12, 2) as float) < 9,\n" +
            "                      concat(substr(create_Date, 12, 2), '~',\n" +
            "                             concat('0', cast(cast(substr(create_Date, 12, 2) as float) + 1 as string))),\n" +
            "                      concat(substr(create_Date, 12, 2), '~',\n" +
            "                             cast(cast(substr(create_Date, 12, 2) as float) + 1 as string))) as time_period,\n" +
            "                   now()                                                                     as upload_time\n" +
            "            from ods_machine_extract.wb_Xtreme\n" +
            "            where create_date > '";

    public static final String COMBINE_WIRE_USAGE_SQL_SUFFIX = "' and (wire_turn_count <> \"0\"\n" +
            "                or unitbondtotal <> \"0\")\n" +
            "              and lotname <> \"\") ta\n" +
            "               inner join data_analysis_db.ems_basic_info_view tb\n" +
            "                          on ta.device_id = tb.device_id) tc\n" +
            "where tc.wire_usage > 0\n" +
            "  and tc.yield > 0\n" +
            "order by create_date";

    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String MYSQL_JDBC_URL = "jdbc:mysql://10.170.6.40:32125/qtech_biz_1?serverTimezone=Asia/Shanghai&useSSL=false&useUnicode=true&allowPublicKeyRetrieval=true";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PWD = "root";

    public static final String MYSQL_TABLE_JOB_DT_SQL = "select pre_run_time from qtech_biz_1.qtech_job_run_dt where program_name = 'wb_wire_usage'";

    public static final String MYSQL_TABLE_JOB_DT_VIEW = "job_dt_table";

    public static final String DORIS_JDBC_URL = "jdbc:mysql://10.170.6.40:32333/qtech_biz_2?rewriteBatchedStatements=true&useSSL=false";

    public static final String RES_DORIS_TABLE_NAME = "qtech_biz_2.dwd_wb_wire_usage_detail";

    public static final String DORIS_USER = "spark_wb";

    public static final String DORIS_PWD = "spark_wb";

    public static final String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";

    public static final String POSTGRES_JDBC_URL = "jdbc:postgresql://10.170.6.40:32120/qtech_wb?binaryTransfer=false&forceBinary=false&reWriteBatchedInserts=true";

    public static final String POSTGRES_USER = "qtech_pg_wb";

    public static final String POSTGRES_PWD = "Ft!*NcYe";

    public static final String POSTGRES_TABLE_WARN = "public.job_run_status";

    public static final String POSTGRES_TABLE_JOB_DT_SQL = "select pre_run_time from public.job_run_status where job_name = 'wb_wire_usage'";

    public static final String POSTGRES_TABLE_JOB_DT_VIEW = "job_dt_table";
}
