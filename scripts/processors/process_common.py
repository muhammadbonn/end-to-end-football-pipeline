def process_na_du(df):
    df = df.dropna(how="all")
    df = df.dropDuplicates()
    return df

def process_teams(df):
    df = df.withColumnRenamed("teamID", "team_id")
    df = df.orderBy("team_id")
    return df
    
def process_players(df):
    df = df.withColumnRenamed("playerID", "player_id")
    df = df.orderBy("player_id")
    return df

def process_leagues(df):
    df = df.withColumnRenamed("leagueID", "league_id")
    df = df.withColumnRenamed("understatNotation", "understat_notation")
    df = df.orderBy("league_id")
    return df