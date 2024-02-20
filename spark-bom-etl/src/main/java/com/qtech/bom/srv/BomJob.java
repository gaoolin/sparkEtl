package com.qtech.bom.srv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/05/13 10:24:35
 * @description : AA BOM 数据同步
 */


public class BomJob {

    private static final Logger logger = LoggerFactory.getLogger(BomJob.class);

    public static void main(String[] args) {

        long timeBegin = System.currentTimeMillis();

        logger.info(">>>>> 开始AA BOM ETL任务！");

        new Bom().start();

        long timeEnd = System.currentTimeMillis();

        logger.info(">>>>> 作业完成，使用时长：{}秒！", Math.round((timeEnd - timeBegin) / 1000.0));
    }
}
