from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FPS = 25


@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.gold_possessions_all",
    comment="Possession sequences across all play states with cumulative time",
    partition_cols=["match_id"],
)
def gold_possessions_all():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.silver_positions")

    possession_frames_all = silver_df \
        .filter(F.col("has_possession") == True) \
        .filter(F.col("team_id") != "BALL")

    possession_frames_all = possession_frames_all \
        .withColumn("row_num", F.row_number().over(Window.partitionBy("match_id", "game_section").orderBy("frame_id"))) \
        .withColumn("row_num_team", F.row_number().over(Window.partitionBy("match_id", "game_section", "team_id").orderBy("frame_id"))) \
        .withColumn("group_id", F.col("row_num") - F.col("row_num_team"))

    possession_sequences_all = possession_frames_all.groupBy("match_id", "game_section", "team_id", "group_id") \
        .agg(
            F.min("frame_id").alias("start_frame"),
            F.max("frame_id").alias("end_frame"),
            F.countDistinct("frame_id").alias("num_frames"),
        ) \
        .withColumn("duration_sec", F.col("num_frames") / F.lit(FPS)) \
        .withColumn("duration_min", F.round(F.col("duration_sec") / 60, 2)) \
        .withColumn("cumulative_time", F.concat(
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) / 60).cast("int").cast("string"), 2, "0"),
            F.lit(":"),
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) % 60).cast("int").cast("string"), 2, "0")
        )) \
        .withColumn("row_num", F.row_number().over(Window.partitionBy("match_id").orderBy("start_frame"))) \
        .select("row_num", "match_id", "game_section", "team_id", "group_id", "start_frame", "end_frame", "num_frames", "duration_sec", "duration_min", "cumulative_time")

    return possession_sequences_all