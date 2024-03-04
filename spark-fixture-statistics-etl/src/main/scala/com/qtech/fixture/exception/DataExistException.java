package com.qtech.fixture.exception;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2024/03/04 11:48:13
 * desc   :
 */


public class DataExistException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    protected final String message;

    public DataExistException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
