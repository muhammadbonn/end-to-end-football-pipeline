-- ==========================================
-- Set Context for Gold Layer
-- ==========================================
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS ete_football_db;
USE DATABASE ete_football_db;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- ==========================================
-- 1. STRIKER PERFORMANCE ANALYSIS
-- ==========================================
CREATE OR REPLACE TABLE GOLD_STRIKER_PERFORMANCE AS
SELECT 
    p.name AS player_name,
    COALESCE(l.name, 'Unknown League') AS league_name, 
    SUM(a.GOALS) AS total_goals,
    ROUND(SUM(a.X_GOALS), 2) AS total_xg,
    ROUND(SUM(a.GOALS) - SUM(a.X_GOALS), 2) AS goals_diff_xg,
    ROUND(AVG(a.SHOTS), 2) AS avg_shots_per_game
FROM Staging.APPEARANCES a 
JOIN Staging.PLAYERS p ON TRY_CAST(a.player_id AS INT) = TRY_CAST(p.player_id AS INT)
LEFT JOIN Staging.LEAGUES l ON TRY_CAST(a.league_id AS INT) = TRY_CAST(l.league_id AS INT)
GROUP BY 1, 2
HAVING total_goals >= 3
ORDER BY goals_diff_xg DESC;

-- ==========================================
-- 2. TEAM DOMINANCE REPORT
-- ==========================================
CREATE OR REPLACE TABLE GOLD_TEAM_DOMINANCE AS
SELECT 
    t.name AS team_name,
    ts.location,
    COUNT(*) AS games_played,
    SUM(CASE WHEN ts.result = 'win' THEN 3 WHEN ts.result = 'draw' THEN 1 ELSE 0 END) AS total_points,
    ROUND(AVG(ts.XG), 2) AS avg_xg_created, 
    ROUND(AVG(ts.goals), 2) AS avg_goals_scored
FROM Staging.TEAMSTATS ts
JOIN Staging.TEAMS t ON ts.team_id = t.team_id
GROUP BY 1, 2
ORDER BY total_points DESC;

-- ==========================================
-- 3. SHOT EFFICIENCY ANALYSIS
-- ==========================================
CREATE OR REPLACE TABLE GOLD_SHOT_ANALYSIS AS
SELECT 
    shot_zone,
    shot_type,
    situation,
    COUNT(*) AS total_shots,
    SUM(CASE WHEN shot_result = 'goal' THEN 1 ELSE 0 END) AS total_goals,
    ROUND((SUM(CASE WHEN shot_result = 'goal' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS conversion_rate_pct
FROM Staging.SHOTS
GROUP BY 1, 2, 3
ORDER BY conversion_rate_pct DESC;
