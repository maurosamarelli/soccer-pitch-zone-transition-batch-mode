from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, avg, when, broadcast, row_number, abs as spark_abs,
    round, lag, max as spark_max, sqrt, pow as spark_pow,
    create_map, lit, concat_ws, element_at, concat,
)
from pyspark.sql.window import Window


@dp.temporary_view()
def attack_directions():
    positions = spark.read.table("`bundesliga-2022-2023`.batch.raw_positions")
    match_info = spark.read.table("`bundesliga-2022-2023`.batch.match_info")
    teams = match_info.select(
        col("match_id"), col("home_team_id").alias("team_id"),
    ).unionAll(
        match_info.select(col("match_id"), col("guest_team_id").alias("team_id")),
    ).distinct()
    half_starts = [("firstHalf", 10000), ("secondHalf", 100000)]
    gk_rows = []
    for half, start_frame in half_starts:
        player_avgs = positions.filter(
            col("game_section") == half
        ).filter(
            col("frame_id").between(start_frame, start_frame + 200)
        ).join(
            broadcast(teams), ["match_id", "team_id"],
        ).groupBy("match_id", "game_section", "team_id", "person_id").agg(
            avg("x").alias("avg_x"),
        )
        w = Window.partitionBy("match_id", "game_section", "team_id").orderBy(spark_abs(col("avg_x")).desc())
        gk = player_avgs.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
        gk_rows.append(gk)
    all_gk = gk_rows[0].unionByName(gk_rows[1])
    return all_gk.select(
        "match_id", "game_section", "team_id",
        when(col("avg_x") < 0, 1).otherwise(-1).alias("attacking_direction"),
    )


@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.bronze_positions",
    comment="Enriched tracking data with attacking direction, normalized coordinates, ball distance, pitch zones, and prev-frame columns",
    partition_cols=["match_id"],
)
def bronze_positions():
    positions = spark.read.table("`bundesliga-2022-2023`.batch.raw_positions")
    match_info = spark.read.table("`bundesliga-2022-2023`.batch.match_info")

    # Join with attack_directions (non-BALL rows get direction)
    bronze_df = positions.join(
        broadcast(spark.read.table("attack_directions")),
        on=["match_id", "game_section", "team_id"],
        how="left",
    )
    # Join with match_info for pitch dims and home/guest team IDs
    bronze_df = bronze_df.join(
        broadcast(match_info.select("match_id", "pitch_x", "pitch_y", "home_team_id", "guest_team_id")),
        on=["match_id"],
        how="left",
    )

    # For BALL rows: resolve attacking_direction from the possessing team via window
    w_frame = Window.partitionBy("match_id", "frame_id")
    home_ad = spark_max(when(col("team_id") == col("home_team_id"), col("attacking_direction"))).over(w_frame)
    guest_ad = spark_max(when(col("team_id") == col("guest_team_id"), col("attacking_direction"))).over(w_frame)
    bronze_df = bronze_df.withColumn(
        "attacking_direction",
        when(col("team_id") == "BALL",
            when(col("ball_possession") == 1, home_ad)
            .when(col("ball_possession") == 2, guest_ad)
            .otherwise(None)
        ).otherwise(col("attacking_direction"))
    )

    # Add x_norm, y_norm (attack-normalized coordinates)
    bronze_df = bronze_df.withColumn(
        "x_norm",
        round(
            when(
                col("attacking_direction") == 1,
                col("x") + col("pitch_x") / 2
            ).otherwise(
                col("pitch_x") - (col("x") + col("pitch_x") / 2)
            ),
            2
        )
    ).withColumn(
        "y_norm",
        round(
            when(
                col("attacking_direction") == 1,
                col("y") + col("pitch_y") / 2
            ).otherwise(
                col("pitch_y") - (col("y") + col("pitch_y") / 2)
            ),
            2
        )
    ).drop("pitch_x", "pitch_y", "home_team_id", "guest_team_id")

    # Add ball_distance
    w_ball = Window.partitionBy("match_id", "frame_id")
    bronze_df = bronze_df.withColumn(
        "ball_x", spark_max(when(col("team_id") == "BALL", col("x"))).over(w_ball)
    ).withColumn(
        "ball_y", spark_max(when(col("team_id") == "BALL", col("y"))).over(w_ball)
    ).withColumn(
        "ball_distance",
        round(sqrt(spark_pow(col("x") - col("ball_x"), 2) + spark_pow(col("y") - col("ball_y"), 2)), 2)
    ).drop("ball_x", "ball_y")

    # Add pitch_zone and zone_id (9-cell classification)
    PITCH_X = 105.0
    PITCH_Y = 68.0
    x_third = (
        when(col("x_norm") < PITCH_X / 3, lit("first-third"))
        .when(col("x_norm") < 2 * PITCH_X / 3, lit("second-third"))
        .otherwise(lit("final-third"))
    )
    y_zone = (
        when(col("y_norm") < PITCH_Y / 3, lit("left"))
        .when(col("y_norm") < 2 * PITCH_Y / 3, lit("centre"))
        .otherwise(lit("right"))
    )
    zone_map = create_map(
        lit("first-third_left"), lit(1),
        lit("first-third_centre"), lit(2),
        lit("first-third_right"), lit(3),
        lit("second-third_left"), lit(4),
        lit("second-third_centre"), lit(5),
        lit("second-third_right"), lit(6),
        lit("final-third_left"), lit(7),
        lit("final-third_centre"), lit(8),
        lit("final-third_right"), lit(9),
    )
    bronze_df = bronze_df.withColumn(
        "pitch_zone", concat(x_third, lit("_"), y_zone)
    ).withColumn(
        "zone_id", zone_map[col("pitch_zone")]
    )

    # Add prev_ columns
    w_lag = Window.partitionBy("match_id", "person_id").orderBy("frame_id")
    prev_cols = [
        "frame_id", "timestamp", "x", "y", "z",
        "speed", "distance", "acceleration",
        "ball_possession", "ball_status",
        "attacking_direction", "x_norm", "y_norm", "ball_distance",
        "pitch_zone"
    ]
    for c in prev_cols:
        bronze_df = bronze_df.withColumn(f"prev_{c}", lag(col(c)).over(w_lag))

    bronze_df = bronze_df.withColumn(
        "_non_consecutive",
        col("prev_frame_id").isNull() | (col("prev_frame_id") != (col("frame_id") - 1))
    )
    for c in prev_cols:
        bronze_df = bronze_df.withColumn(
            f"prev_{c}", when(col("_non_consecutive"), None).otherwise(col(f"prev_{c}"))
        )
    bronze_df = bronze_df.drop("_non_consecutive")
    bronze_df = bronze_df.filter(col("prev_frame_id").isNotNull())

    return bronze_df
