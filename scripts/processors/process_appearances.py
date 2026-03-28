from pyspark.sql.functions import col, when

def process_appearances(df):
    # Rename columns
    df = df \
        .withColumnRenamed("gameID", "game_id") \
        .withColumnRenamed("playerID", "player_id") \
        .withColumnRenamed("goals", "goals") \
        .withColumnRenamed("ownGoals", "own_goals") \
        .withColumnRenamed("shots", "shots") \
        .withColumnRenamed("xGoals", "x_goals") \
        .withColumnRenamed("assists", "assists") \
        .withColumnRenamed("keyPasses", "key_passes") \
        .withColumnRenamed("positionOrder", "position_order") \
        .withColumnRenamed("yellowCard", "yellow_card") \
        .withColumnRenamed("redCard", "red_card")

    # Clean position
    df = df.withColumn(
        "position",
        when(col("position") == "GK", "goalkeeper")
        .when(col("position") == "DF", "defender")
        .when(col("position") == "DC", "center_back")
        .when(col("position") == "DL", "left_back")
        .when(col("position") == "DR", "right_back")
        .when(col("position") == "MF", "midfielder")
        .when(col("position") == "FW", "forward")
        .otherwise("other")
    )

    return df