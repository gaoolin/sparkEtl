package com.qtech;

import static com.qtech.comparison.utils.Utils.getJobRunDt;
import static org.junit.Assert.assertTrue;

import com.qtech.comparison.utils.Utils;
import com.qtech.etl.utils.DateUtils;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * Unit test for simple App.
 */
public class AppTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void testTime() {
        System.out.println(DateUtils.dateTimeNow("yyyy-MM-dd HH:mm:ss"));

        System.out.println(Utils.offsetTime(DateUtils.dateTimeNow("yyyy-MM-dd HH:mm:ss"), 15));
    }

    @Test
    public void testbatis() {
        String wb_comparison_result = getJobRunDt("wb_comparison_result");
        System.out.println(wb_comparison_result);

        System.out.println(String.format("asdf  %s", "aaabbbccc"));
    }

}
