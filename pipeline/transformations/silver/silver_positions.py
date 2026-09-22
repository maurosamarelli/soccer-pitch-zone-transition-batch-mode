from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, collect_list, struct, expr, when,
    max as spark_max, round, broadcast,
    element_at, create_map, lit, sqrt,
)
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.silver_positions",
    comment="Grouped tracking data with players struct, offside line, play state, possession flag, possession zone, ball distance target, and frame score",
    partition_cols=["match_id"],
)
def silver_positions():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.bronze_positions")
    match_info = spark.read.table("`bundesliga-2022-2023`.batch.match_info")

    # Filter out referee rows
    silver_df = silver_df.filter(col("team_id") != "referee")
    player_cols = [
        "person_id", "x", "y", "z", "speed", "distance", "acceleration",
        "x_norm", "y_norm", "ball_distance", "attacking_direction",
        "ball_possession", "ball_status",
        "prev_x", "prev_y", "prev_speed", "prev_ball_distance",
        "prev_x_norm", "prev_y_norm",
        "pitch_zone", "zone_id",
    ]
    silver_grouped = silver_df.groupBy(
        "match_id", "team_id", "frame_id", "game_section", "timestamp",
    ).agg(
        collect_list(struct(*[col(c) for c in player_cols])).alias("players"),
    ).withColumn(
        "players",
        expr("array_sort(players, (a, b) -> CASE WHEN a.x_norm < b.x_norm THEN -1 WHEN a.x_norm > b.x_norm THEN 1 ELSE 0 END)"),
    )
    # Add offside_line and offside_line_perc (join for pitch_x)
    silver_grouped = silver_grouped.join(
        broadcast(match_info.select("match_id", "pitch_x", "home_team_id", "guest_team_id")),
        on=["match_id"],
        how="left",
    )
    silver_grouped = silver_grouped.withColumn(
        "offside_line", expr("get(players, 1).x_norm"),
    ).withColumn(
        "offside_line_perc",
        round(col("offside_line") / col("pitch_x") * 100, 2),
    )

    # Add play_state (window on ball_status)
    w = Window.partitionBy("match_id", "frame_id")
    silver_grouped = silver_grouped.withColumn(
        "play_state",
        when(
            spark_max(when(col("team_id") == "BALL", expr("get(players, 0).ball_status"))).over(w) == 1,
            "active",
        ).otherwise("interruption"),
    )

    # Add has_possession (using joined home/guest team IDs)
    ball_poss = spark_max(when(col("team_id") == "BALL", expr("get(players, 0).ball_possession"))).over(w)
    silver_grouped = silver_grouped.withColumn(
        "has_possession",
        when(
            (ball_poss == 1) & (col("team_id") == col("home_team_id")),
            True,
        ).when(
            (ball_poss == 2) & (col("team_id") == col("guest_team_id")),
            True,
        ).otherwise(False),
    ).drop("pitch_x", "home_team_id", "guest_team_id")

    # Add possession_zone (mirror map for opponent perspective)
    raw_zone = spark_max(when(col("team_id") == "BALL", expr("get(players, 0).pitch_zone"))).over(w)
    mirror_map = create_map(
        lit("first-third_left"),    lit("final-third_right"),
        lit("first-third_centre"),  lit("final-third_centre"),
        lit("first-third_right"),   lit("final-third_left"),
        lit("second-third_left"),   lit("second-third_right"),
        lit("second-third_centre"), lit("second-third_centre"),
        lit("second-third_right"),  lit("second-third_left"),
        lit("final-third_left"),    lit("first-third_right"),
        lit("final-third_centre"),  lit("first-third_centre"),
        lit("final-third_right"),   lit("first-third_left"),
    )
    silver_grouped = silver_grouped.withColumn(
        "possession_zone",
        when((col("has_possession") == False) & (col("team_id") != "BALL"), mirror_map[raw_zone])
        .otherwise(raw_zone),
    )

    # Add ball_distance_target, prev_ball_distance_target, weight, frame_score
    TARGET_X = 105.0
    TARGET_Y = 34.0
    PITCH_X = 105.0
    target_point = create_map(lit("x"), lit(TARGET_X), lit("y"), lit(TARGET_Y))
    opp_target_point = create_map(lit("x"), lit(PITCH_X - TARGET_X), lit("y"), lit(TARGET_Y))

    bx = expr("get(players, 0).x_norm")
    by = expr("get(players, 0).y_norm")
    pbx = expr("get(players, 0).prev_x_norm")
    pby = expr("get(players, 0).prev_y_norm")
    tx = target_point.getItem("x")
    ty = target_point.getItem("y")
    ox = opp_target_point.getItem("x")
    oy = opp_target_point.getItem("y")

    silver_grouped = silver_grouped.withColumn(
        "_dist_attacking",
        when(col("team_id") == "BALL",
            round(sqrt((bx - tx) * (bx - tx) + (by - ty) * (by - ty)), 2)
        )
    ).withColumn(
        "_dist_defending",
        when(col("team_id") == "BALL",
            round(sqrt((bx - ox) * (bx - ox) + (by - oy) * (by - oy)), 2)
        )
    ).withColumn(
        "_prev_dist_attacking",
        when(col("team_id") == "BALL",
            round(sqrt((pbx - tx) * (pbx - tx) + (pby - ty) * (pby - ty)), 2)
        )
    ).withColumn(
        "_prev_dist_defending",
        when(col("team_id") == "BALL",
            round(sqrt((pbx - ox) * (pbx - ox) + (pby - oy) * (pby - oy)), 2)
        )
    )

    silver_grouped = silver_grouped.withColumn(
        "_dist_attacking", spark_max(col("_dist_attacking")).over(w)
    ).withColumn(
        "_dist_defending", spark_max(col("_dist_defending")).over(w)
    ).withColumn(
        "_prev_dist_attacking", spark_max(col("_prev_dist_attacking")).over(w)
    ).withColumn(
        "_prev_dist_defending", spark_max(col("_prev_dist_defending")).over(w)
    )

    silver_grouped = silver_grouped.withColumn(
        "ball_distance_target",
        when(col("has_possession") == True, col("_dist_attacking"))
        .when(col("team_id") == "BALL", col("_dist_attacking"))
        .otherwise(col("_dist_defending"))
    ).withColumn(
        "prev_ball_distance_target",
        when(col("has_possession") == True, col("_prev_dist_attacking"))
        .when(col("team_id") == "BALL", col("_prev_dist_attacking"))
        .otherwise(col("_prev_dist_defending"))
    ).drop("_dist_attacking", "_dist_defending", "_prev_dist_attacking", "_prev_dist_defending")

    silver_grouped = silver_grouped \
        .withColumn("weight_ball_distance_target", F.round(1.0 / (1.0 + col("ball_distance_target")), 4)) \
        .withColumn("prev_weight_ball_distance_target", F.round(1.0 / (1.0 + col("prev_ball_distance_target")), 4)) \
        .withColumn("frame_score", F.round(col("weight_ball_distance_target") - col("prev_weight_ball_distance_target"), 6))

    return silver_grouped
