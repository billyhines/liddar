WITH maxdaynums AS (
    SELECT 
        t1_teamid,
        MAX(daynum) AS max_daynum,
        MAX(season) AS season
    FROM training_data_sdv
    WHERE season = SEASON_PLACEHOLDER 
      AND daynum < DAYNUM_PLACEHOLDER
    GROUP BY t1_teamid
)
SELECT
    t.Season,
    t.DayNum,
    t.T1_TeamID,
    t.T1_FGMmean,
    t.T1_FGAmean,
    t.T1_FGM3mean,
    t.T1_FGA3mean,
    t.T1_ORmean,
    t.T1_Astmean,
    t.T1_TOmean,
    t.T1_Stlmean,
    t.T1_PFmean,
    t.T1_PointDiffmean,
    t.T1_opponent_FGMmean,
    t.T1_opponent_FGAmean,
    t.T1_opponent_FGM3mean,
    t.T1_opponent_FGA3mean,
    t.T1_opponent_ORmean,
    t.T1_opponent_Astmean,
    t.T1_opponent_TOmean,
    t.T1_opponent_Stlmean,
    t.T1_opponent_Blkmean,
    t.T1_win_ratio_14d
FROM maxdaynums m
LEFT JOIN training_data_sdv t 
    ON t.t1_teamid = m.t1_teamid 
   AND m.max_daynum = t.daynum 
   AND m.season = t.season;
