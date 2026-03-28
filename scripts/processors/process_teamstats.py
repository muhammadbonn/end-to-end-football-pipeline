from pyspark.sql.functions import col, when, year, month

def process_teamstats(df):
    df = df.withColumn("yellowCards", col("yellowCards").cast("int"))
    
    df = df \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date"))
    
    df = df \
        .withColumnRenamed("gameID", "game_id") \
        .withColumnRenamed("teamID", "team_id") \
        .withColumnRenamed("xGoals", "x_goals") \
        .withColumnRenamed("shotsOnTarget", "shots_on_target") \
        .withColumnRenamed("yellowCards", "yellow_cards") \
        .withColumnRenamed("redCards", "red_cards")
    
    df = df.withColumn(
        "location",
        when(col("location") == "h", "home")
        .when(col("location") == "a", "away")
        .otherwise("unknown"))
    
    df = df.withColumn(
        "result",
        when(col("result") == "W", "win")
        .when(col("result") == "L", "loss")
        .when(col("result") == "D", "draw"))
    return df