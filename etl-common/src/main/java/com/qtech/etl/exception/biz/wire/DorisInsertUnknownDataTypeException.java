package com.qtech.etl.exception.biz.wire;

import com.qtech.etl.exception.biz.SparkException;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/05/10 16:30:12
 * @description :
 */


public class DorisInsertUnknownDataTypeException extends SparkException {

    private static final long serialVersionUID = 1L;

    public DorisInsertUnknownDataTypeException() { super("数据报错到StarRocks时出错，未知的Dataset数据类型，请检查！");
    }

    public DorisInsertUnknownDataTypeException(String message) {
        super(message);
    }
}
