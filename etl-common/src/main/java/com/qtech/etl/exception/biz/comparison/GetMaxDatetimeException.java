package com.qtech.etl.exception.biz.comparison;

import com.qtech.etl.exception.base.BaseException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/05/26 09:39:15
 * desc   :
 */


public class GetMaxDatetimeException extends NullPointerException {

    private static final String defaultMessage = "获取数据集的最大时间失败，未匹配到时间字段";
    public GetMaxDatetimeException(String s) {
        super(s);
    }

    public GetMaxDatetimeException() {
        super(defaultMessage);
    }
}
