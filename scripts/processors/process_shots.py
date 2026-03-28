from pyspark.sql.functions import when, sqrt, pow, col

def process_shots(df):
    df = df \
    .withColumnRenamed("gameID", "game_id") \
    .withColumnRenamed("shooterID", "shooter_id") \
    .withColumnRenamed("assisterID", "assister_id") \
    .withColumnRenamed("lastAction", "last_action") \
    .withColumnRenamed("shotType", "shot_type") \
    .withColumnRenamed("shotResult", "shot_result") \
    .withColumnRenamed("xGoal", "XG") \
    .withColumnRenamed("positionX", "position_x") \
    .withColumnRenamed("positionY", "position_y")

    #
    df = df.replace({
        "Goal": "goal",
        "Throughball": "through_ball",
        "GoodSkill": "good_skill",
        "HeadPass": "head_pass",
        "Dispossessed": "dispossessed",
        "Punch": "punch",
        "BallTouch": "ball_touch",
        "Save": "save",
        "LayOff": "layoff",
        "Challenge": "challenge",
        "PenaltyFaced": "penalty_faced",
        "BlockedPass": "blocked_pass",
        "Foul": "foul",
        "Clearance": "clearance",
        "Rebound": "rebound",
        "Card": "card",
        "End": "end",
        "SubstitutionOn": "substitution_on",
        "CornerAwarded": "corner_awarded"
    }, subset=["last_action"])

    #
    df = df.replace({
        "OpenPlay": "open_play",
        "FromCorner": "corner",
        "DirectFreekick": "direct_free_kick",
        "SetPiece": "set_piece",
        "Penalty": "penalty"
    }, subset=["situation"])

    df = df.replace({
        "Head": "head",
        "RightFoot": "right_foot",
        "OtherBodyPart": "other_body_part",
        "LeftFoot": "left_foot"
    }, subset=["shot_type"])
    
    df = df.replace({
        "Goal": "goal",
        "OwnGoal": "own_goal",
        "MissedShots": "missed",
        "BlockedShot": "blocked",
        "SavedShot": "saved",
        "ShotOnPost": "on_post"
    }, subset=["shot_result"])

    # distance from goal
    df = df.withColumn(
        "shot_distance",
        sqrt(pow(col("position_x") - 1, 2) + pow(col("position_y") - 0.5, 2))
    )
    
    # classify shot zone
    df = df.withColumn(
        "shot_zone",
        when(col("position_x") > 0.8, "danger")
        .when(col("position_x") > 0.5, "medium")
        .otherwise("low")
    )
    return df

