package com.qtech.comparison.ibatis.mapper;

import com.qtech.comparison.ibatis.pojo.JobRunInfo;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/13 16:32:57
 * desc   :  TODO
 */


public class JobRunInfoMapperTest {
    private InputStream is = null;

    {
        try {
            is = Resources.getResourceAsStream("mybatis-config.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getJobRunDt() throws IOException {
        SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBuilder.build(is, "druid_oracle");
        SqlSession sqlSession = sqlSessionFactory.openSession(true);

//        JobRunDt o = sqlSession.selectOne("com.qtech.comparison.ibatis.mapper.JobRunDtMapper.selectById", 1);

        JobRunInfoMapper mapper = sqlSession.getMapper(JobRunInfoMapper.class);
        JobRunInfo jobRunInfo = mapper.getJobRunDt("wb_comparison_result");

//        System.out.println(o);
        System.out.println(jobRunInfo);

    }
}
