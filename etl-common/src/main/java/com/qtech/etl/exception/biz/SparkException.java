package com.qtech.etl.exception.biz;

/**
 * @author : gaozhilin
 * @project : qtech-data-etl
 * @email : gaoolin@gmail.com
 * @date : 2023/04/18 15:40:08
 * @description :
 */


public class SparkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected final String message;

    public SparkException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
