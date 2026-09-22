from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FPS = 25


@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.gold_possession_zones",
    comment="Zone-level possession sequences with team and opponent metrics, possession_id and stage_id",
    partition_cols=["match_id"],
)
def gold_possession_zones():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.silver_positions")

    all_team_frames = silver_df.filter(F.col("team_id") != "BALL")

    w_frame = Window.partitionBy("match_id", "frame_id")
    all_team_frames = all_team_frames \
        .withColumn("_opp_frame_score",
            F.sum(F.when(F.col("has_possession") == False, F.col("frame_score")).otherwise(F.lit(0))).over(w_frame)
        ) \
        .withColumn("_opp_possession_zone",
            F.max(F.when(F.col("has_possession") == False, F.col("possession_zone")).otherwise(F.lit(None))).over(w_frame)
        ) \
        .withColumn("_opp_ball_distance_target",
            F.max(F.when(F.col("has_possession") == False, F.col("ball_distance_target")).otherwise(F.lit(None))).over(w_frame)
        ) \
        .withColumn("_opp_offside_line",
            F.max(F.when(F.col("has_possession") == False, F.col("offside_line")).otherwise(F.lit(None))).over(w_frame)
        ) \
        .withColumn("_opp_offside_line_perc",
            F.max(F.when(F.col("has_possession") == False, F.col("offside_line_perc")).otherwise(F.lit(None))).over(w_frame)
        ) \
        .withColumn("_opp_team_id",
            F.max(F.when(F.col("has_possession") == False, F.col("team_id")).otherwise(F.lit(None))).over(w_frame)
        )

    zone_frames = all_team_frames.filter(F.col("has_possession") == True)

    zone_frames = zone_frames \
        .withColumn("row_num", F.row_number().over(Window.partitionBy("match_id", "game_section").orderBy("frame_id"))) \
        .withColumn("row_num_zone", F.row_number().over(Window.partitionBy("match_id", "game_section", "possession_zone", "team_id").orderBy("frame_id"))) \
        .withColumn("group_id", F.col("row_num") - F.col("row_num_zone"))

    zone_sequences = zone_frames.groupBy("match_id", "game_section", "team_id", "possession_zone", "group_id") \
        .agg(
            F.min("frame_id").alias("start_frame"),
            F.max("frame_id").alias("end_frame"),
            F.countDistinct("frame_id").alias("num_frames"),
            F.sum(F.when(F.col("play_state") == "active", 1).otherwise(0)).alias("active_frames"),
            F.sum(F.when(F.col("play_state") == "interruption", 1).otherwise(0)).alias("interruption_frames"),
            F.round(F.sum("frame_score"), 4).alias("cumulative_score"),
            F.round(F.sum("_opp_frame_score"), 4).alias("opponent_cumulative_score"),
            F.max("_opp_possession_zone").alias("opponent_possession_zone"),
            F.min("ball_distance_target").alias("min_ball_distance_target"),
            F.min("_opp_ball_distance_target").alias("opponent_min_ball_distance_target"),
            F.round(F.avg(F.when(F.expr("get(players, 0).distance") > 0, F.expr("get(players, 0).speed"))), 2).alias("avg_ball_speed"),
            F.round(F.avg("offside_line"), 2).alias("avg_offside_line"),
            F.round(F.avg("offside_line_perc"), 2).alias("avg_offside_line_perc"),
            F.round(F.avg("_opp_offside_line"), 2).alias("opponent_avg_offside_line"),
            F.round(F.avg("_opp_offside_line_perc"), 2).alias("opponent_avg_offside_line_perc"),
            F.max("_opp_team_id").alias("opponent_id"),
        ) \
        .withColumn("duration_sec", F.col("num_frames") / F.lit(FPS)) \
        .withColumn("duration_min", F.round(F.col("duration_sec") / 60, 2)) \
        .withColumn("cumulative_time", F.concat(
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) / 60).cast("int").cast("string"), 2, "0"),
            F.lit(":"),
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) % 60).cast("int").cast("string"), 2, "0")
        )) \
        .withColumn("_prev_team", F.lag("team_id").over(Window.partitionBy("match_id", "game_section").orderBy("start_frame"))) \
        .withColumn("_is_new_poss", F.when(F.col("_prev_team").isNull() | (F.col("team_id") != F.col("_prev_team")), 1).otherwise(0)) \
        .withColumn("possession_id", F.sum("_is_new_poss").over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
        .drop("_prev_team", "_is_new_poss") \
        .withColumn("stage_id", F.row_number().over(Window.partitionBy("match_id", "game_section").orderBy("start_frame"))) \
        .withColumn("team_metrics", F.struct(
            F.col("possession_zone").alias("possession_zone"),
            F.col("cumulative_score").alias("cumulative_score"),
            F.col("min_ball_distance_target").alias("min_ball_distance_target"),
            F.col("avg_ball_speed").alias("avg_ball_speed"),
            F.col("avg_offside_line").alias("avg_offside_line"),
            F.col("avg_offside_line_perc").alias("avg_offside_line_perc"),
        )) \
        .withColumn("opponent_metrics", F.struct(
            F.col("opponent_possession_zone").alias("possession_zone"),
            F.col("opponent_cumulative_score").alias("cumulative_score"),
            F.col("opponent_min_ball_distance_target").alias("min_ball_distance_target"),
            F.col("opponent_avg_offside_line").alias("avg_offside_line"),
            F.col("opponent_avg_offside_line_perc").alias("avg_offside_line_perc"),
        )) \
        .select("possession_id", "stage_id", "match_id", "game_section", "cumulative_time",
                "team_id", "opponent_id",
                "start_frame", "end_frame", "num_frames", "duration_sec", "duration_min",
                "active_frames", "interruption_frames",
                "team_metrics", "opponent_metrics")

    return zone_sequences