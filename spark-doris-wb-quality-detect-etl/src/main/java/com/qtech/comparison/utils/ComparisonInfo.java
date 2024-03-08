package com.qtech.comparison.utils;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/14 14:22:16
 * desc   :  常量类
 */


public enum ComparisonInfo {

    DRUID_RUN_TIME_PATH("hdfs://nameservice/jobs/spark/angleStdModels/RunDate.txt", 0.0f, 0),
    MAIL_USER("bigdata.it@qtechglobal.com", 0.0f, 0),
    MAIL_PWD("qtech2020", 0.0f, 9),
    MAIL_TO_USERS("zhilin.gao@qtechglobal.com,zhilin.gao@qtechglobal.com", 0.0f, 0),
    MAIL_HOST("123.58.177.49", 0.0f, 0),
    MAIL_PORT("25", 0.0f, 0),
    DATASOURCE_DRUID_DORIS("druid_doris", 0.0f, 0),
    DATASOURCE_DRUID_POSTGRES("druid_postgres", 0.0f, 0),
    STD_MOD_SQL("select a.mc_id std_mc_id, line_no std_line_no, lead_x std_lead_x, lead_y std_lead_y, pad_x std_pad_x, pad_y std_pad_y, lead_threshold, pad_threshold, lead_diff std_lead_diff, pad_diff std_pad_diff, wire_len std_wire_len from public.std_mod_detail a left join public.std_mod_info b on a.mc_id = b.mc_id where b.status = 1", 0.0f, 0),
    JOB_RUN_DT("select pre_run_time from public.job_run_status where job_name = '%s'", 0.0f, 0),
    SECONDS_UNIT("", 1000.0F, 0),
    RUNTIME_LAG_MINUTES("", 0.0f, 15),
    SHOW_COUNT("", 0.0f, 0),
    DRUID_RAW_DATA_SQL("select SIMID, ECount, receive_date, MachineType, __time, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, Sendcount from t_dev_attrs where __time > '%s' and receive_date is not null and MachineType is not null and device_type = 'WB' and B1 is not null and B1 not like 'DIE%%' and B1 not like 'LEAD%%' and B1 not like '%%j%%' and B1 not like '.0%%' and B1 not like '%%ln%%'", 0.0f, 0),
    DRUID_RAW_DATA_SQL_TEST("select SIMID, ECount, receive_date, MachineType, __time, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, Sendcount from t_dev_attrs where  __time between '2023-11-08 04:38:26' and '2023-11-08 04:46:26'  and receive_date is not null and MachineType is not null and MachineType like 'C30T13%%' and device_type = 'WB' and B1 is not null and B1 not like 'DIE%%' and B1 not like 'LEAD%%' and B1 not like '%%j%%' and B1 not like '.0%%' and B1 not like '%%ln%%'", 0.0f, 0),
    DRUID_TRANSFORM_SQL("SELECT SIMID as sim_id, MachineType_ as mc_id, __time as dt, case when split(result_b, ',')[0] is not null and split(result_b, ',')[0] !='' then cast( split(result_b, ',')[0] as integer) else 0 end as line_no, case when result_b is not null and result_b !='' then cast( split(result_b, ',')[3] as string) else '0' end as lead_x, case when result_b is not null and result_b !='' then cast( split(result_b, ',')[4] as string) else '0' end as lead_y, case when result_b is not null and result_b !='' then cast( split(result_b, ',')[1] as string) else '0' end as pad_x, case when result_b is not null and result_b !='' then cast( split(result_b, ',')[2] as string) else '0' end as pad_y, ECount as check_port, Sendcount as pieces_index FROM (  SELECT SIMID, ECount, receive_date, trim(MachineType) as MachineType_, __time, Sendcount, stack(10, 'B1', B1, 'B2', B2, 'B3', B3, 'B4', B4, 'B5', B5, 'B6', B6, 'B7', B7, 'B8', B8, 'B9', B9, 'B10', B10) as (B, result_b) from t_dev_attrs) t", 0.0f, 0),
    WB_COMPARISON_REDIS_KEY_PREFIX("wb:comparison:simId:",0.0f, 0),
    POSTGRESQL_JDBC_URL("jdbc:postgresql://10.170.6.40:32120/qtech_wb_comparison?binaryTransfer=false&forceBinary=false&reWriteBatchedInserts=true", 0.0f, 0);

    private final String str;
    private final float flt;
    private final int num;

    ComparisonInfo(String str, float flt, int num) {
        this.str = str;
        this.flt = flt;
        this.num = num;
    }

    public String getStr() {
        return str;
    }

    public float getFlt() {
        return flt;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return "ComparisonInfo{" +
                "str='" + str + '\'' +
                ", flt=" + flt +
                ", num=" + num +
                '}';
    }
}
