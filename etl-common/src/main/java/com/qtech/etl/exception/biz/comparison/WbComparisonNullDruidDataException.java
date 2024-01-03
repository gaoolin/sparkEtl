package com.qtech.etl.exception.biz.comparison;

import com.qtech.etl.exception.base.BaseException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/05/25 10:07:33
 * desc   :
 */


public class WbComparisonNullDruidDataException extends BaseException {

    private static final String defaultMessage = "获取德鲁伊数据为空！";

    public WbComparisonNullDruidDataException(String defaultMessage) {
        super("WbComparison ETL", "401", defaultMessage);
    }

    public WbComparisonNullDruidDataException() {
        super("WbComparison ETL", "401", defaultMessage);
    }
}
