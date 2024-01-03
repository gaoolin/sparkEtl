package com.qtech.etl.exception.biz;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/08 13:34:41
 * desc   :
 */


public class UnknownDataTypeException extends SparkException {

    private static final long serialVersionUID = 1L;

    public UnknownDataTypeException(String message) {
        super(message);
    }

    public UnknownDataTypeException() {
        super("插入数据库报错，未知的数据类型！");
    }
}
