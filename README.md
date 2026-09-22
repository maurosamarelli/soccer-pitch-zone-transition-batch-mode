# soccer-pitch-zone-transition-batch-mode

Evaluate a football match by sequences of ball and players movements segregated on different pitch zones when all data are already available (batch mode).

**Data source:** IDSSE data (2022-2023 Bundesliga / 2. Bundesliga)  
**Tracking source:** TRACAB (25 FPS)  
**Number of matches:** 7  
**Categories:** match information, ball events, ball and players tracking  
**File format:** nested XML structure converted in JSONL with a preliminary step  
**Architecture:** AWS S3 storage + Declarative Lakehouse Tables Pipeline on Databricks

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Data Flow](#architecture--data-flow)
3. [Repository Structure](#repository-structure)
4. [Pipeline Layers](#pipeline-layers)
5. [Raw Positions Transformation Steps](#raw-positions-transformation-steps)
6. [Bronze Transformation Steps](#bronze-transformation-steps)
7. [Silver Transformation Steps](#silver-transformation-steps)
8. [Gold Possession Zones — Transformation Steps](#gold-possession-zones--transformation-steps)
9. [Key Concepts](#key-concepts)
10. [Glossary](#glossary)

---

## Project Overview

This project processes football tracking data from the 2022-2023 Bundesliga season to analyze **possession zone transitions** — how the ball moves between pitch zones while a team is in possession. The pipeline transforms raw JSONL tracking data (25 FPS, \~23.7M rows across 7 matches) through bronze, silver, and gold layers, producing zone-level possession sequences with team and opponent metrics.

Each match contains \~3.4M tracking rows covering players, ball, and referees with X/Y/Z positions, speed, distance, acceleration, and ball possession status at every frame.

---

## Architecture & Data Flow

```
S3 (JSONL/XML)
    │
    ▼
┌─────────────────┐     ┌──────────────────┐
│  raw_positions   │     │  match_info       │
│  (15 cols)       │     │  (match metadata) │
│  23.7M rows      │     │  7 rows           │
└────────┬────────┘     └────────┬─────────┘
         │                        │
         ▼                        ▼
┌─────────────────────────────────────┐
│  bronze_positions (36 cols)          │
│  + attacking_direction               │
│  + x_norm / y_norm (attack-normalized)│
│  + ball_distance                     │
│  + pitch_zone / zone_id (9-cell grid)│
│  + prev_* (lag window, 15 cols)      │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  silver_positions (16 cols)          │
│  + players (struct array, sorted)    │
│  + offside_line / offside_line_perc │
│  + play_state (active/interruption)  │
│  + has_possession (boolean)          │
│  + possession_zone (mirrored)        │
│  + ball_distance_target              │
│  + weight / frame_score              │
└────────────────┬────────────────────┘
                 │
         ┌───────┴────────┐
         ▼                ▼
┌──────────────┐  ┌────────────────────┐
│ gold_pos-    │  │ gold_possession_    │
│ sessions_all │  │ zones (16 cols)    │
│ (11 cols)    │  │ + team_metrics      │
│ possession   │  │ + opponent_metrics   │
│ sequences    │  │ zone-level sequences│
└──────────────┘  └────────────────────┘
```

**Compute:** Databricks Serverless (Spark)  
**Pipeline:** Lakeflow Spark Declarative Pipelines (SDP) with `@dp.materialized_view` decorators  
**Partitioning:** All tables partitioned by `match_id` for efficient per-match queries

---

## Repository Structure

```
soccer-pitch-zone-transition-batch-mode/
├── README.md
├── notebooks/
│   ├── raw_positions.ipynb          # JSONL → Delta table (15 cols)
│   ├── match_information.ipynb      # XML match metadata → Delta table
│   ├── bronze_positions.ipynb       # Bronze enrichment (36 cols)
│   ├── silver_positions.ipynb       # Silver grouping & metrics (16 cols)
│   ├── gold_play_state.ipynb        # Gold possession & zone sequences
│   └── raw_events.ipynb             # Event data loading
└── pipeline/transformations/
    ├── bronze/
    │   └── bronze_positions.py      # SDP materialized view
    ├── silver/
    │   └── silver_positions.py      # SDP materialized view
    └── gold/
        ├── gold_possessions_all.py  # Possession sequences (all play states)
        ├── gold_possession_zones.py # Zone-level sequences with metrics
        └── gold_possession_streaks.py  # Deprecated (superseded)
```

---

## Pipeline Layers

| Layer | Table | Cols | Rows | Purpose |
| --- | --- | --- | --- | --- |
| Raw | `batch.raw_positions` | 15 | 23,739,462 | Tracking data from S3 JSONL (25 FPS) |
| Raw | `batch.match_info` | 20+ | 7 | Match metadata (teams, pitch dims, weather) |
| Bronze | `batch.bronze_positions` | 36 | 23,739,068 | Attack-normalized coords, pitch zones, prev-frame |
| Silver | `batch.silver_positions` | 16 | 3,007,890 | Grouped per team/frame with possession metrics |
| Gold | `batch.gold_possessions_all` | 11 | 2,861 | Team possession sequences (all play states) |
| Gold | `batch.gold_possession_zones` | 16 | 9,095 | Zone-level sequences with team/opponent structs |

---

## Raw Positions Transformation Steps

Source: `s3://bundesliga-2022-2023-data/timemajor/*.jsonl` (7 files, one per match) → Output: `batch.raw_positions` (15 columns, \~23.7M rows)

| Step | Cell | Action | Columns / Result |
| --- | --- | --- | --- |
| 0 | 3 | Read all JSONL files from S3 via `spark.read.json()` | `raw_df`: 2 top-level columns (`frames` array, `n` line number). Each line = one frame with an array of entities. |
| 1 | 4 | Explode `frames` array and flatten into one row per entity per frame. Cast all string fields to proper types (long, double, timestamp, int). | `positions_df`: 15 columns — `frame_id`, `match_id`, `game_section`, `team_id`, `person_id`, `timestamp`, `x`, `y`, `z`, `speed`, `distance`, `acceleration`, `m_flag`, `ball_possession`, `ball_status` |
| 2 | 5 | Save `positions_df` as Delta table `batch.raw_positions` (overwrite mode). | 23,739,462 rows, 15 columns, 7 matches. Partitioned by `match_id` in downstream use. |

**Output schema (15 columns):**

| # | Column | Type | Source Field | Description |
| --- | --- | --- | --- | --- |
| 1 | `frame_id` | `bigint` | `n` | Sequential frame number within a match. Primary frame identifier (25 FPS). |
| 2 | `match_id` | `string` | `FrameSet.MatchId` | Unique match identifier (e.g. `DFL-MAT-J03WMX`). FK to `match_info`. |
| 3 | `game_section` | `string` | `FrameSet.GameSection` | `firstHalf` or `secondHalf`. Teams switch sides at halftime. |
| 4 | `team_id` | `string` | `FrameSet.TeamId` | Home team ID, guest team ID, `BALL`, or `referee`. |
| 5 | `person_id` | `string` | `FrameSet.PersonId` | Unique entity identifier (e.g. `DFL-OBJ-0002HE`). `BALL` for ball rows. |
| 6 | `timestamp` | `timestamp` | `Frame.T` | Wall-clock time of the frame. |
| 7 | `x` | `double` | `Frame.X` | Pitch X in meters. **Absolute**: \~-52.5 to +52.5, center at 0. Goal-to-goal axis. |
| 8 | `y` | `double` | `Frame.Y` | Pitch Y in meters. \~-34 to +34, center at 0. Sideline-to-sideline axis. |
| 9 | `z` | `double` | `Frame.Z` | Height in meters (ball height, player jumps). Null for most rows. |
| 10 | `speed` | `double` | `Frame.S` | Entity speed in m/s. |
| 11 | `distance` | `double` | `Frame.D` | Distance since previous frame in meters. `0` when ball is dead. |
| 12 | `acceleration` | `double` | `Frame.A` | Acceleration in m/s². |
| 13 | `m_flag` | `int` | `Frame.M` | Manual correction flag — tracking data was manually adjusted. |
| 14 | `ball_possession` | `int` | `Frame.BallPossession` | `1` = home team, `2` = guest team. Only set on BALL rows. |
| 15 | `ball_status` | `int` | `Frame.BallStatus` | `1` = alive (in play), `0` = dead (stoppage). `0` implies `distance=0`. |

**Key relationships** (verified in validation cells below):
* Coordinates are **absolute** — teams switch ends at halftime, so X positions flip sign between halves
* `ball_possession` is always 1 or 2 on every BALL row (even when dead)
* `ball_status=0` implies `distance=0` (always true)
* `Frame.N` is not loaded — `frame_id` (from `n`) is the sole frame identifier
* \~23.7M rows across 7 matches, \~3.4M rows per match

---

## Bronze Transformation Steps

Input: `raw_positions` (15 columns, \~23.7M rows) → Output: `bronze_positions` (36 columns, \~23.7M rows)

| Step | Cell | Action | Columns Added / Modified |
| --- | --- | --- | --- |
| 0 | 3 | Load `raw_positions` from Delta table | 15 input columns loaded |
| 1 | 4 | Compute attacking direction per team per half via goalkeeper X position | `directions` DataFrame (temp, 28 rows) |
| 2 | 5 | Build `create_map` lookup from directions (28 entries, key = `match_id\|game_section\|team_id`) | `attack_map` (temp map expression) |
| 3 | 6 | Add `attacking_direction` to every row. BALL rows resolved via `ball_possession` (1=home, 2=guest) → possessing team's direction | `attacking_direction` added (col 16) |
| 4 | 7 | Add `x_norm`, `y_norm` — attack-normalized coordinates (0 = own goal, pitch dim = opponent goal). Flips based on `attacking_direction` | `x_norm`, `y_norm` added (cols 17–18) |
| 5 | 8 | Add `ball_distance` — Euclidean distance from each entity to the ball at the same frame (window over `match_id`, `frame_id`) | `ball_distance` added (col 19) |
| 6 | 9 | Add `pitch_zone` (9-cell grid: 3 X-thirds × 3 Y-lanes) and `zone_id` (1–9) based on `x_norm` / `y_norm` | `pitch_zone`, `zone_id` added (cols 20–21) |
| 7 | 10 | Add `prev_` columns via `lag()` window (partitioned by `match_id`, `person_id`, ordered by `frame_id`). Nullified on non-consecutive frames. First-frame rows filtered out (394 rows). | `prev_frame_id`, `prev_timestamp`, `prev_x`, `prev_y`, `prev_z`, `prev_speed`, `prev_distance`, `prev_acceleration`, `prev_ball_possession`, `prev_ball_status`, `prev_attacking_direction`, `prev_x_norm`, `prev_y_norm`, `prev_ball_distance`, `prev_pitch_zone` (cols 22–36) |

**Output schema (36 columns):**

| # | Column | Type | Source |
| --- | --- | --- | --- |
| 1–15 | `frame_id` ... `ball_status` | various | Passthrough from `raw_positions` |
| 16 | `attacking_direction` | int | Step 3 — create_map lookup + BALL resolution |
| 17–18 | `x_norm`, `y_norm` | double | Step 4 — attack-normalized coordinates |
| 19 | `ball_distance` | double | Step 5 — Euclidean distance to ball |
| 20–21 | `pitch_zone`, `zone_id` | string, int | Step 6 — 9-cell pitch classification |
| 22–36 | `prev_*` (15 cols) | various | Step 7 — lag() window, nullified on gaps |

---

## Silver Transformation Steps

Input: `bronze_positions` (36 columns, \~23.7M rows) → Output: `silver_positions` (16 columns, \~3M rows)

| Step | Cell | Action | Columns Added / Modified |
| --- | --- | --- | --- |
| 0 | 3 | Load `bronze_positions` from Delta table | 36 input columns loaded |
| 1 | 4 | Filter out `referee` rows (862,572 rows removed) | No new columns (rows reduced to \~22.9M) |
| 2 | 5 | Group by `match_id`, `team_id`, `frame_id`, `game_section`, `timestamp`. Collect players into sorted `players` struct array (21 fields per player, sorted by `x_norm` ascending). | `players` added (col 6). 6 columns total, \~3M rows (1 row per team per frame). |
| 3 | 6 | Add `offside_line` = `players[1].x_norm` (second-lowest X = first outfield player) and `offside_line_perc` = `offside_line / pitch_x * 100`. Uses `create_map` for pitch_x lookup (no join). | `offside_line`, `offside_line_perc` added (cols 7–8) |
| 4 | 7 | Add `play_state` via window on `ball_status`: `1` → `"active"`, `0` → `"interruption"`. Propagated from BALL row to all teams at the same frame. | `play_state` added (col 9) |
| 5 | 8 | Add `has_possession` (boolean) via window on `ball_possession`: `1` → home team `True`, `2` → guest team `True`. Uses `create_map` for home/guest team ID lookup (no join). | `has_possession` added (col 10) |
| 6 | 9 | Add `possession_zone` — BALL's `pitch_zone` from window, mirrored for the opponent team (first-third ↔ final-third, left ↔ right). Possessing team and BALL keep raw zone. | `possession_zone` added (col 11) |
| 7 | 10 | Define target points: `target_point` (105, 34) = attacking goal line centre, `opp_target_point` (0, 34) = opponent goal line centre. Stored as Spark maps. | Temp: `target_point`, `opp_target_point` |
| 8 | 11 | Add `ball_distance_target`, `prev_ball_distance_target` — Euclidean distance from ball to target goal (attacking for possessing team, defending for opponent). Propagated via window. Add `weight_ball_distance_target`, `prev_weight_ball_distance_target` (inverse: `1/(1+d)`), and `frame_score` (weight diff). | `ball_distance_target`, `prev_ball_distance_target`, `weight_ball_distance_target`, `prev_weight_ball_distance_target`, `frame_score` added (cols 12–16) |

**Output schema (16 columns):**

| # | Column | Type | Source |
| --- | --- | --- | --- |
| 1–5 | `match_id`, `team_id`, `frame_id`, `game_section`, `timestamp` | various | Group keys from bronze |
| 6 | `players` | array\<struct\> | Step 2 — collect_list of 21 player fields, sorted by x_norm |
| 7–8 | `offside_line`, `offside_line_perc` | double | Step 3 — players[1].x_norm + create_map pitch_x |
| 9 | `play_state` | string | Step 4 — window on ball_status |
| 10 | `has_possession` | boolean | Step 5 — window on ball_possession + create_map home/guest |
| 11 | `possession_zone` | string | Step 6 — BALL pitch_zone + mirror map for opponent |
| 12–13 | `ball_distance_target`, `prev_ball_distance_target` | double | Step 8 — Euclidean to goal, window-propagated |
| 14–15 | `weight_ball_distance_target`, `prev_weight_ball_distance_target` | double | Step 8 — inverse distance `1/(1+d)` |
| 16 | `frame_score` | double | Step 8 — weight diff (current − prev) |

---

## Gold Possession Zones — Transformation Steps

Input: `silver_positions` (16 columns, \~3M rows) → Output: `gold_possession_zones` (16 columns, \~9K sequences)

| Step | Cell | Action | Columns Added / Modified |
| --- | --- | --- | --- |
| 0 | 2 | Load `silver_positions` from Delta table | 16 input columns loaded |
| 1 | 8 | Filter to team rows only (`team_id != "BALL"`). Propagate opponent metrics via window per frame: `_opp_frame_score`, `_opp_possession_zone`, `_opp_ball_distance_target`, `_opp_offside_line`, `_opp_offside_line_perc`, `_opp_team_id`. | 6 temp `_opp_*` columns added via window |
| 2 | 8 | Filter to `has_possession == True` (possessing team rows only). | Rows reduced to possessing team frames only |
| 3 | 8 | Gaps-and-islands: `row_num` (per match+section, ordered by frame_id) minus `row_num_zone` (per match+section+zone+team, ordered by frame_id) = `group_id`. Breaks when `possession_zone` or `team_id` changes. | `group_id` computed (temp) |
| 4 | 8 | Aggregate per group (`match_id`, `game_section`, `team_id`, `possession_zone`, `group_id`): `start_frame`, `end_frame`, `num_frames`, `active_frames`, `interruption_frames`, `cumulative_score`, opponent metrics, `min_ball_distance_target`, `avg_ball_speed`, `avg_offside_line`, `avg_offside_line_perc`, `opponent_id`. | 17 aggregated columns |
| 5 | 8 | Add `duration_sec` (`num_frames / 25`), `duration_min`, `cumulative_time` (running sum of duration per match+section, formatted `MM:SS`). | `duration_sec`, `duration_min`, `cumulative_time` added |
| 6 | 8 | Add `possession_id` via lag: increment when `team_id` changes between consecutive sequences (running sum of `_is_new_poss`). | `possession_id` added |
| 7 | 8 | Add `stage_id` via `row_number()` per match+section ordered by `start_frame`. | `stage_id` added |
| 8 | 8 | Build `team_metrics` struct (possession_zone, cumulative_score, min_ball_distance_target, avg_ball_speed, avg_offside_line, avg_offside_line_perc) and `opponent_metrics` struct (same fields from opponent). | `team_metrics`, `opponent_metrics` structs added |

**Output schema (16 columns):**

| # | Column | Type | Source |
| --- | --- | --- | --- |
| 1 | `possession_id` | int | Step 6 — lag-based, increments on team change |
| 2 | `stage_id` | int | Step 7 — row_number per match+section |
| 3 | `match_id` | string | Group key |
| 4 | `game_section` | string | Group key |
| 5 | `cumulative_time` | string | Step 5 — running duration `MM:SS` |
| 6 | `team_id` | string | Group key — possessing team |
| 7 | `opponent_id` | string | Step 4 — from opponent window |
| 8 | `start_frame` | long | Step 4 — min frame_id in group |
| 9 | `end_frame` | long | Step 4 — max frame_id in group |
| 10 | `num_frames` | long | Step 4 — count distinct frames |
| 11 | `duration_sec` | double | Step 5 — num_frames / 25 FPS |
| 12 | `duration_min` | double | Step 5 — duration_sec / 60 |
| 13 | `active_frames` | long | Step 4 — play_state = active count |
| 14 | `interruption_frames` | long | Step 4 — play_state = interruption count |
| 15 | `team_metrics` | struct | Step 8 — possession_zone, cumulative_score, min_ball_distance_target, avg_ball_speed, avg_offside_line, avg_offside_line_perc |
| 16 | `opponent_metrics` | struct | Step 8 — opponent's mirrored metrics |

---

## Key Concepts

### Attack-Normalized Coordinates (`x_norm`, `y_norm`)

Raw tracking coordinates are **absolute** — teams physically switch ends at halftime, so a player's X position flips sign between halves. To make all analysis direction-agnostic, the bronze layer computes `x_norm` and `y_norm` where `0` always represents the possessing team's own goal and `pitch_dim` represents the opponent's goal. This is done by detecting each team's **attacking direction** (`+1` or `-1`) from the goalkeeper's average X position in the first 200 frames of each half.

### 9-Cell Pitch Zone Classification (`pitch_zone`, `zone_id`)

The pitch is divided into a 3×3 grid based on attack-normalized coordinates:

| | Left | Centre | Right |
| --- | --- | --- | --- |
| **Final-third** (70–105) | `final-third_left` (7) | `final-third_centre` (8) | `final-third_right` (9) |
| **Second-third** (35–70) | `second-third_left` (4) | `second-third_centre` (5) | `second-third_right` (6) |
| **First-third** (0–35) | `first-third_left` (1) | `first-third_centre` (2) | `first-third_right` (3) |

For the opponent team, zones are **mirrored**: first-third ↔ final-third, left ↔ right.

### Frame Score & Weighted Ball Distance

The `frame_score` measures how much the ball approached or retreated from the attacking target goal (105, 34) in a single frame:

- `ball_distance_target` — Euclidean distance from the ball to the goal target
- `weight = 1 / (1 + distance)` — inverse distance (closer = higher weight)
- `frame_score = weight - prev_weight` — positive when approaching, negative when retreating

The score is **not specular** between teams because the weight function `1/(1+d)` is convex: teams closer to their target get larger weight changes for the same distance delta.

### Gaps-and-Islands Pattern

Consecutive frames with the same `possession_zone` and `team_id` are grouped into sequences using the classic gaps-and-islands technique: `row_number()` over the full partition minus `row_number()` over the sub-partition (zone + team) produces a constant `group_id` for each consecutive run. Breaks occur when the ball moves to a different zone or possession changes hands.

### `possession_id` vs `stage_id`

- `possession_id` — increments via `lag()` whenever `team_id` changes between consecutive zone sequences (identifies which team's overall possession this zone belongs to)
- `stage_id` — simple `row_number()` per match+section ordered by `start_frame` (sequential zone number within a half)

---

## Glossary

| Term | Definition |
| --- | --- |
| **Frame** | One snapshot of tracking data at 25 FPS. Identified by `frame_id` (from JSONL field `n`). |
| **Entity** | Any tracked object: player, ball, or referee. Identified by `team_id` + `person_id`. |
| **Attacking direction** | `+1` = attacks left-to-right, `-1` = attacks right-to-left. Determined by goalkeeper position. |
| **Offside line** | The second-lowest `x_norm` among outfield players (the first outfield player's position). `players[1].x_norm` after sorting by `x_norm` ascending. |
| **Play state** | `active` (ball in play, `ball_status=1`) or `interruption` (ball dead, `ball_status=0`). |
| **Possession zone** | The pitch zone where the ball is located, from the perspective of the possessing team. Mirrored for the opponent. |
| **SDP** | Lakeflow Spark Declarative Pipelines — Databricks pipeline framework using `@dp.materialized_view` decorators. |
| **Gaps-and-islands** | SQL pattern for grouping consecutive rows sharing a property, using `row_number()` difference. |
