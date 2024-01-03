package com.qtech.baseinfo.utils;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/01/02 09:15:12
 * desc   :
 */


public class Constants {
    public static final String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    public static final String IMPALA_JDBC_URL = "jdbc:impala://10.170.3.15:21050/ods_machine_extract;UseSasl=0;AuthMech=3;UID=qtkj;PWD=;characterEncoding=utf-8";

    public static final String DORIS_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String DORIS_JDBC_URL = "jdbc:mysql://10.170.6.40:32333/qtech_eq_ods?rewriteBatchedStatements=true";

    public static final String DORIS_FE_IP = "10.170.6.40";

    public static final String DORIS_FE_PORT = "31210";

}
