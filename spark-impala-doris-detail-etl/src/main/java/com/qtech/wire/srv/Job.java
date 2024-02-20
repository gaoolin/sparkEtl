package com.qtech.wire.srv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/26 11:00:55
 * @description :
 */

/*FIXME 下次任务运行时间有空值的问题*/
public class Job {

    private static final Logger logger = LoggerFactory.getLogger(Job.class.getName());

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        long timeBegin = System.currentTimeMillis();

        logger.warn(">>>>> 开始合并Xtreme和Areo金线使用数据！");

        new CombineXtremeAndAreoWireUsage().start();

        long timeEnd = System.currentTimeMillis();

        logger.warn(">>> 作业完成，使用时长：{}秒！", Math.round((timeEnd - timeBegin) / 1000.0));
    }
}
