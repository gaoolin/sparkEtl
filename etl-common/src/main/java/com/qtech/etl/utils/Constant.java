package com.qtech.etl.utils;

/**
 * @author : gaozhilin
 * @project : qtech-kudu-wire-usage-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/03 13:45:40
 * @description : 应用常量
 */


public class Constant {

    public static final String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    public static final String IMPALA_JDBC_URL = "jdbc:impala://10.170.3.15:21050/ods_machine_extract;UseSasl=0;" +
            "AuthMech=3;UID=qtkj;PWD=;characterEncoding=utf-8";

    public static final String KUDU_MASTER = "10.170.3.11:7051,10.170.3.12:7051,10.170.3.13:7051";

    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String MYSQL_JDBC_URL = "jdbc:mysql://10.170.6.40:32125/qtech_biz_1?serverTimezone=Asia/Shanghai&useSSL=false&useUnicode=true&allowPublicKeyRetrieval=true";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PWD = "root";
}
