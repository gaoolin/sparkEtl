package com.qtech.comparison.algorithm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * Project : WbComparison
 * Author  : zhilin.gao
 * Email   : gaoolin@gmail.com
 * Date    : 2021/12/23 16:28
 */
public class Algorithm {

    /**
     * not standardized model products
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> noProg(Dataset<Row> df) {
        return df.select(col("sim_id"), col("mc_id"), col("dt"))
                .groupBy(col("sim_id"), col("mc_id"))
                .agg(min(col("dt")))
                .withColumnRenamed("min(dt)", "dt")
                .withColumn("code", lit(3))
                .withColumn("description", lit("NO PROGRAM"));
    }

    /**
     * Common part of less-wire or multi-wire data packet
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> lackAndOverGroup(Dataset<Row> df) {
        return df.select(col("sim_id"), col("mc_id"), col("first_draw_time"), col("line_no"),
                col("check_port"), col("std_mod_line_cnt"))
                .groupBy(col("sim_id"), col("mc_id"), col("first_draw_time"))
                .agg(max(col("check_port")), max(col("std_mod_line_cnt")))
                .withColumnRenamed("max(check_port)", "check_port")
                .withColumnRenamed("max(std_mod_line_cnt)", "std_mod_line_cnt")
                .withColumnRenamed("first_draw_time", "dt");
    }

    /**
     * Descriptive information generated from less-wire production
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> lackLn2doc(Dataset<Row> df) {

        Dataset<Row> groupDF = lackAndOverGroup(df);

        return groupDF.withColumn("code", lit(2))
                .withColumn("description", format_string("lackWire: [actual/required: %s/%s]", col("check_port"),
                        when(col("check_port").lt(col("std_mod_line_cnt")), col("std_mod_line_cnt"))
                                .otherwise(col("std_mod_line_cnt").plus(2))))
                .select(col("sim_id"), col("mc_id"), col("dt"), col("code"),
                        col("description"));
    }

    /**
     * Descriptive information generated from multi-wire production
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> overLn2doc(Dataset<Row> df) {
        Dataset<Row> groupDF = lackAndOverGroup(df);

        return groupDF.withColumn("code", lit(4))
                .withColumn("description", format_string("outOfBond: [actual/required:" + "%s/%s" + "]", col("check_port"), when(col("check_port").equalTo(col("std_mod_line_cnt").plus(1)), col("std_mod_line_cnt")).otherwise(col("std_mod_line_cnt").plus(2))))
                .select(col("sim_id"), col("mc_id"), col("dt"), col("code"), col("description"));
    }

    /**
     * Link-wire data to generate full-wire data
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> linkLnEstimate(Dataset<Row> df) {
        WindowSpec winOrdByPIdx = Window.partitionBy(df.col("sim_id"), df.col("mc_id"), df.col("pieces_index"))
                .orderBy(df.col("line_no"));

        WindowSpec winOrdByLnAsc = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"))
                .orderBy(col("line_no").asc());

        WindowSpec winOrdByLnDesc = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"))
                .orderBy(col("line_no").desc());

        Dataset<Row> minMaxDF = df.withColumn("line_no_asc", rank().over(winOrdByLnAsc))
                .withColumn("line_no_desc", rank().over(winOrdByLnDesc))
                .filter("line_no_asc in (1, 2) or line_no_desc in (1, 2)")
                .withColumn("wire_label", when(col("line_no").equalTo(1).or(col("line_no").equalTo(2)), "doubleMin").otherwise("doubleMax"))
                .select(col("sim_id"), col("mc_id"), col("pieces_index"), col("line_no"), col("wire_label"), col("wire_len"));

        Dataset<Row> minMaxMapDF = minMaxDF.select(col("sim_id"), col("mc_id"), col("pieces_index"), col("line_no"), col("wire_label"));

        Dataset<Row> minMaxAggDF = minMaxDF.groupBy(col("sim_id"), col("mc_id"), col("pieces_index"), col("wire_label"))
                .agg(sum(col("wire_len")))
                .withColumnRenamed("sum(wire_len)", "wire_len_ttl")
                .withColumn("wire_len_ttl_rank", rank().over(Window.partitionBy(col("sim_id"), col("mc_id"),
                        col("pieces_index")).orderBy(col("wire_len_ttl").asc())))
                .filter("wire_len_ttl_rank = 1")
                .select(col("sim_id"), col("mc_id"), col("pieces_index"), col("wire_label")).distinct();

        Dataset<Row> deleteLnMapDF = minMaxMapDF.join(minMaxAggDF, minMaxMapDF.col("sim_id").equalTo(minMaxAggDF.col("sim_id"))
                .and(minMaxMapDF.col("mc_id").equalTo(minMaxAggDF.col("mc_id")))
                .and(minMaxMapDF.col("pieces_index").equalTo(minMaxAggDF.col("pieces_index")))
                .and(minMaxMapDF.col("wire_label").equalTo(minMaxAggDF.col("wire_label"))), "inner")
                .withColumn("confirm_label", lit(0))
                .select(minMaxMapDF.col("sim_id"), minMaxMapDF.col("mc_id"), minMaxMapDF.col("pieces_index"),
                        minMaxMapDF.col("line_no"), col("confirm_label"));

        return df.join(deleteLnMapDF, df.col("sim_id").equalTo(deleteLnMapDF.col("sim_id"))
                .and(df.col("mc_id").equalTo(deleteLnMapDF.col("mc_id")))
                .and(df.col("pieces_index").equalTo(deleteLnMapDF.col("pieces_index")))
                .and(df.col("line_no").equalTo(deleteLnMapDF.col("line_no"))), "left")
                .filter("confirm_label is null")
                .withColumn("line_no_mock", rank().over(winOrdByPIdx))
                .select(df.col("sim_id"), df.col("mc_id"), df.col("dt"), col("first_draw_time"),
                        col("line_no_mock"), col("lead_x"), col("lead_y"), col("pad_x"), col("pad_y"),
                        col("check_port"), df.col("pieces_index"), col("sub_mc_id"),
                        col("cnt"), col("wire_len"))
                .withColumnRenamed("line_no_mock", "line_no")
                .sort(col("sim_id"), col("mc_id"), col("first_draw_time"), col("line_no"));
    }

    // FIXME
    //  1-利用pad点位置给线排序 需要修改这里的逻辑, 2022-03-29 done.
    public static Dataset<Row> coordinateNormalization(Dataset<Row> df) {
        WindowSpec winOrdByXYOffset = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"))
                .orderBy(col("lx_ly_offset").asc());

        Dataset<Row> transformDF = df.withColumn("lx_ly_offset", col("pad_x").plus(col("pad_y")))
                .withColumn("lx_ly_offset_rank", rank().over(winOrdByXYOffset));

        Dataset<Row> normalizeUnitDF = transformDF
                .filter("lx_ly_offset_rank = 1")
                .select(col("sim_id"), col("mc_id"), col("pieces_index"), col("pad_x"),
                        col("pad_y"))
                .withColumnRenamed("pad_x", "pad_x_norm_unit")
                .withColumnRenamed("pad_y", "pad_y_norm_unit");

        return transformDF.join(normalizeUnitDF, transformDF.col("sim_id").equalTo(normalizeUnitDF.col("sim_id"))
                .and(transformDF.col("mc_id").equalTo(normalizeUnitDF.col("mc_id")))
                .and(transformDF.col("pieces_index").equalTo(normalizeUnitDF.col("pieces_index"))), "inner")

                .withColumn("lead_x_mock", col("lead_x").minus(col("pad_x_norm_unit")))
                .withColumn("lead_y_mock", col("lead_y").minus(col("pad_y_norm_unit")))
                .withColumn("pad_x_mock", col("pad_x").minus(col("pad_x_norm_unit")))
                .withColumn("pad_mock", col("pad_y").minus(col("pad_y_norm_unit")))
                // .withColumn("DISTANCE2ZERO", sqrt(pow(col("lead_x_mock"), 2)
                // 		.plus(pow(col("lead_y_mock"), 2))))

                .select(transformDF.col("sim_id"), transformDF.col("mc_id"), col("dt"),
                        col("first_draw_time"), col("line_no"), col("lead_x_mock"),
                        col("lead_y_mock"), col("pad_x_mock"), col("pad_mock"),  // col("DISTANCE2ZERO"),
                        col("check_port"), transformDF.col("pieces_index"), col("sub_mc_id"),
                        col("cnt"), col("wire_len"))
                // .withColumn("DISTANCERANK", rank().over(winOrdByDistance2Zero))

                .sort(col("sim_id"), col("mc_id"), col("dt"), col("line_no"));
    }

    // FIXME 数据处理步骤有重复待优化
    public static Dataset<Row> generateLineNmb(Dataset<Row> normalizeDF) {
        WindowSpec winOrdByNormalXYOffset = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"))
                .orderBy(col("normal_xy_offset").desc());

        WindowSpec winLabRnk = Window.partitionBy(col("sim_id"), col("mc_id"), col("pieces_index"), col("radioX<Y"))
                .orderBy(col("normal_xy_offset").asc());

        Dataset<Row> normalizeTransformDF = normalizeDF.withColumn("normal_xy_offset", col("pad_x_mock").plus(col("pad_mock")));

        // 计算出产品右上角的PAD点的线号、坐标、斜率，并分类为0
        Dataset<Row> maxXYOffset = normalizeTransformDF
                .withColumn("normal_xy_offsetRANK", rank().over(winOrdByNormalXYOffset))
                .filter("normal_xy_offsetRANK = 1")
                .withColumn("radioY/X", col("pad_mock").divide(col("pad_x_mock").cast("float")))
                .withColumnRenamed("line_no", "flag_point")
                .select(col("sim_id"), col("mc_id"), col("flag_point"), col("pieces_index"), col("radioY/X"));

        Dataset<Row> joinDF = normalizeTransformDF.join(maxXYOffset, normalizeTransformDF.col("sim_id").equalTo(maxXYOffset.col("sim_id"))
                .and(normalizeTransformDF.col("mc_id").equalTo(maxXYOffset.col("mc_id")))
                .and(normalizeTransformDF.col("pieces_index").equalTo(maxXYOffset.col("pieces_index"))))
                .select(normalizeTransformDF.col("sim_id"), normalizeTransformDF.col("mc_id"), col("dt"),
                        col("first_draw_time"), col("line_no"), col("lead_x_mock"),
                        col("lead_y_mock"), col("pad_x_mock"), col("pad_mock"), col("check_port"),
                        normalizeTransformDF.col("pieces_index"), col("sub_mc_id"), col("cnt"),
                        col("wire_len"), col("normal_xy_offset"), col("radioY/X"), col("flag_point"));  // , col("DISTANCERANK")

        return joinDF.withColumn("radioX<Y", when(col("radioY/X").multiply(col("pad_x_mock"))
                .leq(col("pad_mock")), lit(0)).otherwise(lit(1)))
                .withColumn("radioX<Y", when(col("line_no").equalTo(col("flag_point")), lit(0))
                        .otherwise(col("radioX<Y")))
                .withColumn("label_rank", rank().over(winLabRnk))
                .select(normalizeTransformDF.col("sim_id"), normalizeTransformDF.col("mc_id"), col("dt"),
                        col("first_draw_time"), col("line_no"), col("lead_x_mock"),
                        col("lead_y_mock"), col("pad_x_mock"), col("pad_mock"),
                        col("check_port"), normalizeTransformDF.col("pieces_index"), col("sub_mc_id"),
                        col("cnt"), col("wire_len"), col("radioY/X"),
                        col("radioX<Y"), col("normal_xy_offset"), col("label_rank"))
                .sort(col("sim_id"), col("mc_id"), col("dt"), col("line_no"));
    }

    /**
     * Difference the coordinates of the lead point and pad point of the product and calculate the distance between
     * the corresponding coordinates before and after
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> diffMock(Dataset<Row> df) {
        WindowSpec w_len = Window.partitionBy(col("sim_id"),
                col("mc_id"), col("first_draw_time"), col("pieces_index")).orderBy(col("radioX<Y").asc(), col("label_rank").asc());

        return df.withColumn("lead_x_lag", lag("lead_x_mock", 1).over(w_len))
                .withColumn("lead_y_lag", lag("lead_y_mock", 1).over(w_len))
                .withColumn("pad_x_lag", lag("pad_x_mock", 1).over(w_len))
                .withColumn("pad_y_lag", lag("pad_mock", 1).over(w_len))
                .withColumn("lead_len", sqrt(pow(col("lead_x_mock").minus(col("lead_x_lag")), 2)
                        .plus(pow(col("lead_y_mock").minus(col("lead_y_lag")), 2))))
                .withColumn("pad_len", sqrt(pow(col("pad_x_mock").minus(col("pad_x_lag")), 2)
                        .plus(pow(col("pad_mock").minus(col("pad_y_lag")), 2))))
                .select(col("sim_id"), col("mc_id"), col("dt"), col("first_draw_time"),
                        col("line_no"), col("lead_x_mock"), col("lead_y_mock"), col("pad_x_mock"),
                        col("pad_mock"), col("lead_len"), col("pad_len"), col("check_port"),
                        col("pieces_index"), col("sub_mc_id"), col("wire_len"), col("radioY/X"),
                        col("radioX<Y"), col("normal_xy_offset"), col("label_rank"));
    }


    public static Dataset<Row> diff(Dataset<Row> df) {
        WindowSpec w_len = Window.partitionBy(col("sim_id"),
                col("mc_id"), col("first_draw_time"), col("pieces_index")).orderBy(col("line_no").asc());

        return df.withColumn("lead_x_lag", lag("lead_x", 1).over(w_len))
                .withColumn("lead_y_lag", lag("lead_y", 1).over(w_len))
                .withColumn("pad_x_lag", lag("pad_x", 1).over(w_len))
                .withColumn("pad_y_lag", lag("pad_y", 1).over(w_len))
                .withColumn("lead_len", sqrt(pow(col("lead_x").minus(col("lead_x_lag")), 2)
                        .plus(pow(col("lead_y").minus(col("lead_y_lag")), 2))))
                .withColumn("pad_len", sqrt(pow(col("pad_x").minus(col("pad_x_lag")), 2)
                        .plus(pow(col("pad_y").minus(col("pad_y_lag")), 2))))
                .select(col("sim_id"), col("mc_id"), col("dt"), col("first_draw_time"),
                        col("line_no"), col("lead_x"), col("lead_y"), col("pad_x"),
                        col("pad_y"), col("lead_len"), col("pad_len"), col("check_port"),
                        col("pieces_index"), col("sub_mc_id"), col("wire_len"));
    }


    public static Dataset<Row> fullLnMkStaMock(Dataset<Row> df, Dataset<Row> stdModelDF) {
        Dataset<Row> stdModDF = stdModelDF
                .select(col("std_mc_id"), col("std_line_no"), col("std_lead_x"),
                        col("std_lead_y"), col("std_pad_x"), col("std_pad_y"),
                        col("lead_threshold"), col("pad_threshold"), col("std_wire_len"));

        return df.join(stdModelDF, df.col("sub_mc_id").equalTo(stdModDF.col("std_mc_id")), "left")
                .withColumn("lead_offset", sqrt(pow(col("lead_x_mock").minus(col("std_lead_x")), 2)
                        .plus(pow(col("lead_y_mock").minus(col("std_lead_y")), 2))))
                .withColumn("pad_offset", sqrt(pow(col("pad_x_mock").minus(col("std_pad_x")), 2)
                        .plus(pow(col("pad_mock").minus(col("std_pad_y")), 2))))
                .withColumn("code", when(col("lead_offset").gt(col("lead_threshold"))
                        .or(col("pad_offset").gt(col("pad_threshold"))), 1).otherwise(0));
    }

    /**
     * Full-wire products generate status code
     *
     * @param df         dataframe
     * @param stdModelDF dataframe
     * @return dataframe
     */
    // FIXME
    public static Dataset<Row> fullLnMkStaMockBak(Dataset<Row> df, Dataset<Row> stdModelDF) {
        Dataset<Row> stdmodDF = stdModelDF
                .select(col("std_mc_id"), col("std_line_no"), col("std_lead_diff"),
                        col("std_pad_diff"), col("lead_threshold"), col("pad_threshold"),
                        col("std_wire_len"));

        Dataset<Row> fulllinestateDF = df.join(stdmodDF, (df.col("sub_mc_id").equalTo(col("std_mc_id"))), "left")
                // 原先的偏移量取绝对值， 现在去掉绝对值运算，显示偏移量的方向
                .withColumn("lead_offset", col("lead_len").minus(col("std_lead_diff")))
                .withColumn("pad_offset", col("pad_len").minus(col("std_pad_diff")))
                .withColumn("code", when((col("lead_offset").gt(col("leadThreshold")).or(col("pad_offset").gt(col("pad_threshold")))), 1).otherwise(lit(0)));
        // 用于控制卡控方向的、lead点到pad点之间线长的范围
        // .withColumn("exceededStdLen", col("wire_len").gt(col("stdWireLen").plus(THRESHOLD_VALUE)));

        return fulllinestateDF
                .select(col("sim_id"), col("mc_id"), col("dt"),
                        col("first_draw_time"), col("line_no"), col("lead_x_mock"), col("lead_y_mock"),
                        col("pad_x_mock"), col("pad_mock"), col("lead_len"), col("pad_len"),
                        col("check_port"), col("pieces_index"), col("sub_mc_id"), col("std_lead_diff"),
                        col("std_pad_diff"), col("lead_threshold"), col("pad_threshold"),
                        col("lead_offset"), col("pad_offset"), col("code"));
        // , col("exceededStdLen")
    }


    public static Dataset<Row> fullLnMkSta(Dataset<Row> df, Dataset<Row> stdModelDF) {
        Dataset<Row> stdModDF = stdModelDF
                .select(col("std_mc_id"), col("std_line_no"), col("std_lead_diff"),
                        col("std_pad_diff"), col("lead_threshold"), col("pad_threshold"),
                        col("std_wire_len"));

        Dataset<Row> fulllinestateDF = df.join(stdModDF, (df.col("sub_mc_id").equalTo(col("std_mc_id")).and(df.col("line_no").equalTo(col("std_line_no")))), "left")
                // 原先的偏移量取绝对值， 现在去掉绝对值运算，显示偏移量的方向
                .withColumn("lead_offset", col("lead_len").minus(col("std_lead_diff")))
                .withColumn("pad_offset", col("pad_len").minus(col("std_pad_diff")))
                .withColumn("code", when((abs(col("lead_offset")).gt(col("lead_threshold")).or(abs(col("pad_offset")).gt(col("pad_threshold")))), 1).otherwise(lit(0)));
        // 用于控制卡控方向的、lead点到pad点之间线长的范围
        // .withColumn("exceededStdLen", col("wire_len").gt(col("stdWireLen").plus(THRESHOLD_VALUE)));

        return fulllinestateDF
                .select(col("sim_id"), col("mc_id"), col("dt"),
                        col("first_draw_time"), col("line_no"), col("lead_x"), col("lead_y"),
                        col("pad_x"), col("pad_y"), col("lead_len"), col("pad_len"),
                        col("check_port"), col("pieces_index"), col("sub_mc_id"), col("std_lead_diff"),
                        col("std_pad_diff"), col("lead_threshold"), col("pad_threshold"),
                        col("lead_offset"), col("pad_offset"), col("code"));
    }


    /**
     * Descriptive information generated from full-wire production
     *
     * @param df dataframe
     * @return dataframe
     */
    public static Dataset<Row> fullLn2doc(Dataset<Row> df) {
        Dataset<Row> fullLn2doc = df
                // 对每个产品的线号排序，便于后续生成异常描述时，按照线号从小到大排列
                .sortWithinPartitions(col("sim_id"), col("mc_id"), col("first_draw_time"),
                        col("line_no").asc())
                // 为代码为1的行生成固定格式的错误描述
                .withColumn("description", when(col("code").equalTo(1),
                        format_string("ln%sLeOf%sPaOf%s", col("line_no"),
                                format_number(col("lead_offset"), 1),
                                format_number(col("pad_offset"), 1)))
                        .otherwise("qualified"))
                // 对每个产品分组，并对"code"列求最大值，对报异常的行合并描述信息
                .groupBy(col("sim_id"), col("mc_id"), col("first_draw_time"))
                .agg(max("code").as("code"),
                        concat_ws(";", collect_list(when(col("description").equalTo("qualified"), null)
                                .otherwise(col("description")))).as("description"))
//                .filter("exceededStdLen = 'false'")
                .withColumn("description", when(col("description").equalTo(""), "qualified")
                        .otherwise(col("description")))
                .withColumnRenamed("first_draw_time", "dt");

        return fullLn2doc.select(col("sim_id"), col("mc_id"), col("dt"),
                col("code"), col("description"));
    }
}
