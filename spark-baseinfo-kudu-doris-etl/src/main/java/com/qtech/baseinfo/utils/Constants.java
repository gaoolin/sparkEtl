package com.qtech.baseinfo.utils;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/01/02 09:15:12
 * desc   :
 */


public class Constants {
    public static final String IMPALA_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String IMPALA_JDBC_URL = "jdbc:mysql://10.170.3.231:9030/QT_MODEL_EMS?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true";
    public static final String IMPALA_USER = "imd";
    public static final String IMPALA_PWD = "imd@123";

    public static final String DORIS_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String DORIS_JDBC_URL = "jdbc:mysql://10.170.6.40:32333/qtech_eq_ods?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true";

    public static final String DORIS_FE_IP = "10.170.6.40";

    public static final String DORIS_FE_PORT = "31210";

}
