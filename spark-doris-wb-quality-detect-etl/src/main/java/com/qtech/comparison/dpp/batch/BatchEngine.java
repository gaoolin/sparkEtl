package com.qtech.comparison.dpp.batch;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2022/06/12 10:07:53
 * desc   :
 */


public abstract class BatchEngine {

    protected abstract void srvProcessData() throws Exception;

    public void start() throws Exception {
        this.srvProcessData();
    }
}
