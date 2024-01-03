package com.qtech.etl.exception.biz.comparison;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/09/18 13:57:40
 * desc   :
 */


import com.google.common.base.Strings;

// Exception for Spark DPP process
public class SparkDppException extends Exception {
    public SparkDppException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
    }

    public SparkDppException(Throwable cause) {
        super(cause);
    }

    public SparkDppException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(Strings.nullToEmpty(msg), cause, enableSuppression, writableStackTrace);
    }

    public SparkDppException(String msg) {
        super(Strings.nullToEmpty(msg));
    }
}
