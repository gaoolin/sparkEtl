package com.qtech.bom.utils;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/05/13 10:32:09
 * @description : 常量
 */


public class Constant {

    public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static final String ORACLE_JDBC_URL = "jdbc:oracle:thin:@//10.170.1.30:1521/qtdbdg";

    public static final String ORACLE_USER = "ckmes";

    public static final String ORACLE_PWD = "qt123mes";

    public static final String ORACLE_TABLE_TMP_VIEW = "aa_bom_view";

    public static final String ORACLE_AAB_BOM_SQL = "select distinct id, steps, replace_cpn, upn, source, name, spec, unit, qty, waste, position, status, toplimit, id1, nvl(ta.mdate, tb.mdate) mdate, nvl(tb.muser, ta.muser) muser, is_do, remarks, version, op_code, series_code, part_code, part_name, part_spec, type, type2, pixel, process_spec\n" +
            "from (select id, steps, replace_cpn, upn, source, name, spec, unit, qty, waste, position, status, toplimit, id1, mdate, muser, is_do, remarks, version, op_code, series_code\n" +
            "      from aa_bom ta\n" +
            "      where upn in ('640700000002', '640700000008', '640700000001', '6-9999B2655QT', '640700000004', '640700000005', '6-999901450QT',\n" +
            "                    '640700000007', 'S4201000020F')) ta\n" +
            "         inner join i_material tb on ta.id = tb.part_code\n" +
            "where IS_DO = 1 and length(part_spec) = lengthb(part_spec) and REPLACE_CPN = '主料'";

    public static final String RESULT_STARROCKS_TABLE = "ads_aa_bom_info";

    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String STARROCKS_JDBC_URL = "jdbc:mysql://10.170.6.40:32333/qtech_biz_2?rewriteBatchedStatements=true&useSSL=false";

    public static final String STARROCKS_USER = "root";

    public static final String STARROCKS_PWD = "";

}
