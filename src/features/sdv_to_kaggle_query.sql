CREATE TABLE boxscores_sdv_kagglestyle AS
WITH start_dates AS (
    SELECT 
        season, 
        MIN(game_date) AS season_start_date
    FROM boxscores_sdv
    GROUP BY season 
),
boxscore_with_daynum AS (
    SELECT
        mbb.*,
        start_dates.season_start_date,
        (mbb.game_date - start_dates.season_start_date)::int AS DayNum 
    FROM boxscores_sdv mbb
    JOIN start_dates ON mbb.season = start_dates.season
),
game_results AS (
  SELECT
        game_id, 
        season,
        DayNum,
        -- Game Winner and Losers
        MAX(CASE WHEN team_winner THEN team_id END) AS WTeamID,
        MAX(CASE WHEN team_winner THEN team_score END) AS WScore,
        MAX(CASE WHEN team_winner THEN 
              CASE 
                WHEN team_home_away = 'home' THEN 'H'
                WHEN team_home_away = 'away' THEN 'A'
                ELSE 'N'
              END
           END) AS WLoc,
		MAX(CASE WHEN NOT team_winner THEN team_id END) AS LTeamId,
        MAX(CASE WHEN NOT team_winner THEN team_score END) AS LScore,
		-- Winner Stats
		MAX(CASE WHEN team_winner THEN field_goals_made END) AS WFGM,
        MAX(CASE WHEN team_winner THEN field_goals_attempted END) AS WFGA,
        MAX(CASE WHEN team_winner THEN three_point_field_goals_made END) AS WFGM3,
        MAX(CASE WHEN team_winner THEN three_point_field_goals_attempted END) AS WFGA3,
        MAX(CASE WHEN team_winner THEN free_throws_made END) AS WFTM,
        MAX(CASE WHEN team_winner THEN free_throws_attempted END) AS WFTA,
        MAX(CASE WHEN team_winner THEN offensive_rebounds END) AS WOR,
        MAX(CASE WHEN team_winner THEN defensive_rebounds END) AS WDR,
        MAX(CASE WHEN team_winner THEN assists END) AS WAst,
        MAX(CASE WHEN team_winner THEN turnovers END) AS WTO,
        MAX(CASE WHEN team_winner THEN steals END) AS WStl,
        MAX(CASE WHEN team_winner THEN blocks END) AS WBlk,
        MAX(CASE WHEN team_winner THEN fouls END) AS WPF,
        -- Loser Stats
		MAX(CASE WHEN NOT team_winner THEN field_goals_made END) AS LFGM,
        MAX(CASE WHEN NOT team_winner THEN field_goals_attempted END) AS LFGA,
        MAX(CASE WHEN NOT team_winner THEN three_point_field_goals_made END) AS LFGM3,
        MAX(CASE WHEN NOT team_winner THEN three_point_field_goals_attempted END) AS LFGA3,
        MAX(CASE WHEN NOT team_winner THEN free_throws_made END) AS LFTM,
        MAX(CASE WHEN NOT team_winner THEN free_throws_attempted END) AS LFTA,
        MAX(CASE WHEN NOT team_winner THEN offensive_rebounds END) AS LOR,
        MAX(CASE WHEN NOT team_winner THEN defensive_rebounds END) AS LDR,
        MAX(CASE WHEN NOT team_winner THEN assists END) AS LAst,
        MAX(CASE WHEN NOT team_winner THEN turnovers END) AS LTO,
        MAX(CASE WHEN NOT team_winner THEN steals END) AS LStl,
        MAX(CASE WHEN NOT team_winner THEN blocks END) AS LBlk,
        MAX(CASE WHEN NOT team_winner THEN fouls END) AS LPF
   FROM boxscore_with_daynum
   GROUP BY game_id, season, DayNum
)

SELECT
    Season,
    DayNum,
    WTeamID,
    WScore,
    LTeamID,
    LScore,
    WLoc,
    0 AS NumOT, -- Placeholder, since transformation doesn't calculate it 
    WFGM,
    WFGA,
    WFGM3,
    WFGA3,
    WFTM,
    WFTA,
    WOR,
    WDR,
    WAst,
    WTO,
    WStl,
    WBlk,
    WPF,
    LFGM,
    LFGA,
    LFGM3,
    LFGA3,
    LFTM,
    LFTA,
    LOR,
    LDR,
    LAst,
    LTO,
    LStl,
    LBlk,
    LPF
FROM game_results;
