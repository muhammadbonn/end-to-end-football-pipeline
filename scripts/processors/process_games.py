from pyspark.sql.functions import col, to_timestamp, year, month

def process_games(df):
    # Rename columns
    df = df \
        .withColumnRenamed("gameID", "game_id") \
        .withColumnRenamed("leagueID", "league_id") \
        .withColumnRenamed("homeTeamID", "home_team_id") \
        .withColumnRenamed("awayTeamID", "away_team_id") \
        .withColumnRenamed("homeGoals", "home_goals") \
        .withColumnRenamed("awayGoals", "away_goals") \
        .withColumnRenamed("homeProbability", "home_prob") \
        .withColumnRenamed("drawProbability", "draw_prob") \
        .withColumnRenamed("awayProbability", "away_prob")

    # Convert date
    df = df.withColumn("date", to_timestamp(col("date")))

    # Extract year/month
    df = df \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date"))

    return df