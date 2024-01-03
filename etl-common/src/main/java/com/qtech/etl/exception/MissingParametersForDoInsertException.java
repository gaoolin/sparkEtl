package com.qtech.etl.exception;

import com.qtech.etl.exception.biz.SparkException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/14 11:25:15
 * desc   :
 */


public class MissingParametersForDoInsertException extends SparkException {

    private static final long serialVersionUID = 1L;

    public MissingParametersForDoInsertException(String message) {
        super(message);
    }

    public MissingParametersForDoInsertException() {
        super("缺少参数，请检查是否配置driver, url, table参数！");
    }
}
