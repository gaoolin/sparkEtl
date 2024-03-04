package com.qtech.fixture.utils

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/02/29 13:26:22
 * desc   :
 */


object Constants {

  val MYSQL_JDBC_DRIVER: String = "com.mysql.cj.jdbc.Driver"

  val MYSQL_JDBC_URL: String = "jdbc:mysql://10.170.6.40:32125/qtech_biz_2?serverTimezone=Asia/Shanghai&useSSL=false&useUnicode=true&allowPublicKeyRetrieval=true"

  val MYSQL_USER: String = "qtech_web"

  val MYSQL_PWD: String = "Ee786549"

  val MYSQL_DATE_FLAG: String = "select @date_now = weekofyear(date_format(date_sub(now(), interval 1 day), '%y-%m-%d')) as flag from (select @date_now := weekofyear(date_format(now(), '%y-%m-%d'))) ta"

}
