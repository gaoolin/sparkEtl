package com.qtech.comparison.ibatis.config;

/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2022/5/31 13:46
 */


import java.io.IOException;

public class MapperInstance {

    public static <T> T getMapper(String dataSourceName, Class<T> clazz) throws IOException {

        return new MybatisDataSource().pojoMapperBuilder(dataSourceName, clazz);
    }

    public static <T> T getMapper(Class<T> clazz) throws IOException {
        return new MybatisDataSource().pojoMapperBuilder(clazz);
    }
}
