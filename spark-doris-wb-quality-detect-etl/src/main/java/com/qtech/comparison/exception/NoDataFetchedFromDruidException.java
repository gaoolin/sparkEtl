package com.qtech.comparison.exception;

import com.qtech.etl.exception.biz.SparkException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2022/06/14 13:31:11
 * desc   :  获取数据异常类
 */


public class NoDataFetchedFromDruidException extends SparkException {
    public NoDataFetchedFromDruidException(String message) {
        super(message);
    }

    public NoDataFetchedFromDruidException() {
        super("没有从德鲁伊获取到数据，请检查！");
    }
}
