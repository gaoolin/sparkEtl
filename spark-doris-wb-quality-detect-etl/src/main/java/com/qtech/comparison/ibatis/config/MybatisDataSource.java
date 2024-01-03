package com.qtech.comparison.ibatis.config;

/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2022/5/29 10:04
 */


import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class MybatisDataSource {

    private final InputStream is = Resources.getResourceAsStream("mybatis-config.xml");

    /**
     * @description   使用mybatis.xml中配置的其他环境（数据库）
     * @param
     * @return
     */
    public MybatisDataSource() throws IOException {
    }

    public <T> T pojoMapperBuilder(String environmentId, Class<T> clazz) {

        SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBuilder.build(is, environmentId);
        SqlSession sqlSession = sqlSessionFactory.openSession(true);

        return sqlSession.getMapper(clazz);
    }

    /**
     * @description  使用mybatis.xml中配置的默认环境（数据库）
     * @param clazz
     * @return T
     */
    public <T> T pojoMapperBuilder(Class<T> clazz) {
        SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBuilder.build(is);
        SqlSession sqlSession = sqlSessionFactory.openSession(true);

        return sqlSession.getMapper(clazz);
    }
}
