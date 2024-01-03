package com.qtech.etl.exception;

import com.qtech.etl.exception.base.BaseException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/14 16:36:38
 * desc   :
 */


public class NoSuchAlgorithmException extends BaseException {
    public NoSuchAlgorithmException(String defaultMessage) {
        super(defaultMessage);
    }

    public NoSuchAlgorithmException() {
        super("没有这个md5算法！");
    }
}
