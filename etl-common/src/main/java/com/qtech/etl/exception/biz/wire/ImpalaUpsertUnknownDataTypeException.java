package com.qtech.etl.exception.biz.wire;

import com.qtech.etl.exception.biz.SparkException;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/18 15:43:16
 * @description :
 */


public class ImpalaUpsertUnknownDataTypeException extends SparkException {

    private static final long serialVersionUID = 1L;

    public ImpalaUpsertUnknownDataTypeException() {super("未知的Dataset数据类型，请检查！");}

    public ImpalaUpsertUnknownDataTypeException(String message) {super(message);}
}
