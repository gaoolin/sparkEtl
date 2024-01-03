package com.qtech.comparison.dpp.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;


/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2021/12/24 11:54
 */


public class DataProcess {

    public static Dataset<Row> doProcess(Dataset<Row> rawDF, Dataset<Row> stdMdWireCnt) {

        WindowSpec winByPIdx = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"));
        WindowSpec winMcByPIdx = Window.partitionBy(col("sim_id"), col("pieces_index"));

        Dataset<Row> df = rawDF.withColumn("first_draw_time", min(col("dt")).over(winByPIdx))
                .withColumn("sub_mc_id", split(col("mc_id"), "#").getItem(0))
                .withColumn("cnt", count("line_no").over(winByPIdx))
                .withColumn("mcs_by_pieces_index", approx_count_distinct("sub_mc_id").over(winMcByPIdx));

        Dataset<Row> mergeStdMdDF = df.join(stdMdWireCnt, df.col("sub_mc_id").equalTo(stdMdWireCnt.col("std_mc_id")), "left");

        Dataset<Row> filterDF = mergeStdMdDF.select(col("sim_id"), col("mc_id"), col("dt"),
                col("first_draw_time"), col("line_no"), col("lead_x"), col("lead_y"),
                col("pad_x"), col("pad_y"), col("check_port"), col("pieces_index"),
                col("sub_mc_id"), col("cnt"), col("mcs_by_pieces_index"), col("std_mod_line_cnt"))
                .withColumn("wire_len", sqrt(pow(col("pad_x").minus(col("lead_x")), 2).plus(
                        pow(col("pad_y").minus(col("lead_y")), 2))));

        return filterDF.select(col("sim_id"), col("mc_id"), col("dt"),
                col("first_draw_time"), col("line_no"), col("lead_x"), col("lead_y"),
                col("pad_x"), col("pad_y"), col("check_port"), col("wire_len"),
                col("pieces_index"), col("sub_mc_id"), col("cnt"), col("mcs_by_pieces_index"),
                col("std_mod_line_cnt"))
                .sort(col("sim_id"), col("mc_id"), col("first_draw_time"), col("line_no"));

    }


    public static Dataset<Row> unionDataFrame(ArrayList<Dataset<Row>> seqDF) {

        Dataset<Row> firstDF = seqDF.get(0);
        for (int i = 1; i < seqDF.size(); i++) {
            firstDF = firstDF.union(seqDF.get(i));
        }

        return firstDF.select(col("sim_id"), col("mc_id"), col("dt").cast("string"),
                col("code"), col("description"));
    }
}
