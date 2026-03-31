USE ROLE SYSADMIN; 
CREATE DATABASE IF NOT EXISTS ete_football_db;
USE DATABASE ete_football_db;
CREATE SCHEMA IF NOT EXISTS Staging;
USE SCHEMA Staging;

-- ==========================================
-- 1. LEAGUES TABLE
-- ==========================================
CREATE OR REPLACE TABLE LEAGUES (
    league_id INT,               -- Renamed from leagueID
    name VARCHAR,                -- Kept as is
    understat_notation VARCHAR   -- Renamed from understatNotation
);

-- ==========================================
-- 2. TEAMS TABLE
-- ==========================================
CREATE OR REPLACE TABLE TEAMS (
    team_id INT,                 -- Renamed from teamID
    name VARCHAR                 -- Kept as is
);

-- ==========================================
-- 3. PLAYERS TABLE
-- ==========================================
CREATE OR REPLACE TABLE PLAYERS (
    player_id INT,               -- Renamed from playerID
    name VARCHAR                 -- Kept as is
);

-- ==========================================
-- 4. GAMES TABLE
-- ==========================================
CREATE OR REPLACE TABLE GAMES (
    game_id INT,                 -- Renamed from gameID
    league_id INT,               -- Renamed from leagueID
    season INT,                  -- Kept as is
    date TIMESTAMP,              -- Casted to timestamp
    home_team_id INT,            -- Renamed from homeTeamID
    away_team_id INT,            -- Renamed from awayTeamID
    home_goals INT,              -- Renamed from homeGoals
    away_goals INT,              -- Renamed from awayGoals
    home_prob FLOAT,             -- Renamed from homeProbability
    draw_prob FLOAT,             -- Renamed from drawProbability
    away_prob FLOAT,             -- Renamed from awayProbability
    year INT,                    -- Derived column added in PySpark
    month INT                    -- Derived column added in PySpark
    -- Note: Half-time goals and betting odds columns were dropped in PySpark
);

-- ==========================================
-- 5. SHOTS TABLE
-- ==========================================
CREATE OR REPLACE TABLE SHOTS (
    game_id INT,                 -- Renamed from gameID
    shooter_id INT,              -- Renamed from shooterID
    assister_id INT,             -- Renamed from assisterID
    minute INT,                  -- Kept as is
    situation VARCHAR,           -- Kept as is
    last_action VARCHAR,         -- Renamed from lastAction
    shot_type VARCHAR,           -- Renamed from shotType
    shot_result VARCHAR,         -- Renamed from shotResult
    xG FLOAT,                    -- Kept as is
    position_x FLOAT,            -- Renamed from positionX
    position_y FLOAT,            -- Renamed from positionY
    shot_distance FLOAT,         -- Derived column added in PySpark
    shot_zone VARCHAR            -- Derived column added in PySpark
);

-- ==========================================
-- 6. APPEARANCES TABLE
-- ==========================================
CREATE OR REPLACE TABLE APPEARANCES (
    game_id INT,                 -- Renamed from gameID
    player_id INT,               -- Renamed from playerID
    goals INT,                   -- Kept as is
    own_goals INT,               -- Renamed from ownGoals
    shots INT,                   -- Kept as is
    x_goals FLOAT,               -- Renamed from xGoals
    x_goals_chain FLOAT,         -- Renamed from xGoalsChain
    x_goals_buildup FLOAT,       -- Renamed from xGoalsBuildup
    assists INT,                 -- Kept as is
    key_passes INT,              -- Renamed from keyPasses
    x_assists FLOAT,             -- Renamed from xAssists
    position VARCHAR,            -- Kept as is (Values were cleaned)
    position_order INT,          -- Renamed from positionOrder
    yellow_card INT,             -- Renamed from yellowCard
    red_card INT,                -- Renamed from redCard
    time INT,                    -- Kept as is
    substitute_in INT,           -- Renamed from substituteIn
    substitute_out INT,          -- Renamed from substituteOut
    league_id INT                -- Renamed from leagueID
);

-- ==========================================
-- 7. TEAM STATS TABLE
-- ==========================================
CREATE OR REPLACE TABLE TEAMSTATS (
    game_id INT,                 -- Renamed from gameID
    team_id INT,                 -- Renamed from teamID
    season INT,                  -- Kept as is
    date TIMESTAMP,              -- Casted to timestamp
    location VARCHAR,            -- Kept as is
    goals INT,                   -- Kept as is
    xG FLOAT,                    -- Kept as is
    shots INT,                   -- Kept as is
    shots_on_target INT,         -- Renamed from shotsOnTarget
    deep INT,                    -- Kept as is
    ppda FLOAT,                  -- Kept as is
    fouls INT,                   -- Kept as is
    corners INT,                 -- Kept as is
    yellow_cards INT,            -- Renamed from yellowCards
    red_cards INT,               -- Renamed from redCards
    result VARCHAR,              -- Kept as is
    xG_against FLOAT,            -- Calculated/Joined in PySpark
    xG_diff FLOAT                -- Derived column added in PySpark
);
