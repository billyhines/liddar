WITH season_statistics AS (
    SELECT
        Season,
        T1_TeamID,
        AVG(T1_FGM) AS T1_FGMmean,
        AVG(T1_FGA) AS T1_FGAmean,
        AVG(T1_FGM3) AS T1_FGM3mean,
        AVG(T1_FGA3) AS T1_FGA3mean,
        AVG(T1_OR) AS T1_ORmean,
        AVG(T1_Ast) AS T1_Astmean,
        AVG(T1_TO) AS T1_TOmean,
        AVG(T1_Stl) AS T1_Stlmean,
        AVG(T1_PF) AS T1_PFmean,
        AVG(T2_FGM) AS T2_FGMmean,
        AVG(T2_FGA) AS T2_FGAmean,
        AVG(T2_FGM3) AS T2_FGM3mean,
        AVG(T2_FGA3) AS T2_FGA3mean,
        AVG(T2_OR) AS T2_ORmean,
        AVG(T2_Ast) AS T2_Astmean,
        AVG(T2_TO) AS T2_TOmean,
        AVG(T2_Stl) AS T2_Stlmean,
        AVG(T2_Blk) AS T2_Blkmean,
        AVG(PointDiff) AS T1_PointDiffmean
    FROM
        RECIPRICOL_BOXSCORE_TABLE_NAME_PLACEHOLDER
    WHERE
        Season = SEASON_PLACEHOLDER AND DayNum < DAYNUM_PLACEHOLDER
    GROUP BY
        Season, T1_TeamID
), season_statistics_T1 AS (
    SELECT
        Season,
        T1_TeamID,
        T1_FGMmean,
        T1_FGAmean,
        T1_FGM3mean,
        T1_FGA3mean,
        T1_ORmean,
        T1_Astmean,
        T1_TOmean,
        T1_Stlmean,
        T1_PFmean,
        T1_PointDiffmean,
        T2_FGMmean AS T1_opponent_FGMmean,
        T2_FGAmean AS T1_opponent_FGAmean,
        T2_FGM3mean AS T1_opponent_FGM3mean,
        T2_FGA3mean AS T1_opponent_FGA3mean,
        T2_ORmean AS T1_opponent_ORmean,
        T2_Astmean AS T1_opponent_Astmean,
        T2_TOmean AS T1_opponent_TOmean,
        T2_Stlmean AS T1_opponent_Stlmean,
        T2_Blkmean AS T1_opponent_Blkmean
    FROM
        season_statistics
), season_statistics_T2 AS (
    SELECT
        Season,
        T1_TeamID AS T2_TeamID,
        T1_FGMmean AS T2_FGMmean,
        T1_FGAmean AS T2_FGAmean,
        T1_FGM3mean AS T2_FGM3mean,
        T1_FGA3mean AS T2_FGA3mean,
        T1_ORmean AS T2_ORmean,
        T1_Astmean AS T2_Astmean,
        T1_TOmean AS T2_TOmean,
        T1_Stlmean AS T2_Stlmean,
        T1_PFmean AS T2_PFmean,
        T1_PointDiffmean AS T2_PointDiffmean,
        T2_FGMmean AS T2_opponent_FGMmean,
        T2_FGAmean AS T2_opponent_FGAmean,
        T2_FGM3mean AS T2_opponent_FGM3mean,
        T2_FGA3mean AS T2_opponent_FGA3mean,
        T2_ORmean AS T2_opponent_ORmean,
        T2_Astmean AS T2_opponent_Astmean,
        T2_TOmean AS T2_opponent_TOmean,
        T2_Stlmean AS T2_opponent_Stlmean,
        T2_Blkmean AS T2_opponent_Blkmean
    FROM
        season_statistics
), last14days_stats_T1 AS (
    SELECT
        Season,
        T1_TeamID,
        AVG(CASE WHEN (t1_score-t2_score) > 0 THEN 1 ELSE 0 END) AS T1_win_ratio_14d
    FROM
        RECIPRICOL_BOXSCORE_TABLE_NAME_PLACEHOLDER
    WHERE
        DayNum >= DAYNUM_PLACEHOLDER - 14 AND Season = SEASON_PLACEHOLDER
    GROUP BY
        Season, T1_TeamID
), last14days_stats_T2 AS (
    SELECT
        Season,
        T1_TeamID AS T2_TeamID,
		T1_win_ratio_14d AS T2_win_ratio_14d
    FROM
        last14days_stats_T1
)
SELECT
    b.Season,
    b.DayNum,
    b.location,
    b.T1_TeamID,
    b.T1_Score,
    b.T2_TeamID,
    b.T2_Score,
    s1.T1_FGMmean,
    s1.T1_FGAmean,
    s1.T1_FGM3mean,
    s1.T1_FGA3mean,
    s1.T1_ORmean,
    s1.T1_Astmean,
    s1.T1_TOmean,
    s1.T1_Stlmean,
    s1.T1_PFmean,
    s1.T1_PointDiffmean,
    s1.T1_opponent_FGMmean,
    s1.T1_opponent_FGAmean,
    s1.T1_opponent_FGM3mean,
    s1.T1_opponent_FGA3mean,
    s1.T1_opponent_ORmean,
    s1.T1_opponent_Astmean,
    s1.T1_opponent_TOmean,
    s1.T1_opponent_Stlmean,
    s1.T1_opponent_Blkmean,
    s2.T2_FGMmean,
    s2.T2_FGAmean,
    s2.T2_FGM3mean,
    s2.T2_FGA3mean,
    s2.T2_ORmean,
    s2.T2_Astmean,
    s2.T2_TOmean,
    s2.T2_Stlmean,
	s2.T2_PFmean,
    s2.T2_PointDiffmean,
    s2.T2_opponent_FGMmean,
    s2.T2_opponent_FGAmean,
    s2.T2_opponent_FGM3mean,
    s2.T2_opponent_FGA3mean,
    s2.T2_opponent_ORmean,
    s2.T2_opponent_Astmean,
    s2.T2_opponent_TOmean,
    s2.T2_opponent_Stlmean,
    s2.T2_opponent_Blkmean,
    l1.T1_win_ratio_14d,
    l2.T2_win_ratio_14d
FROM
    RECIPRICOL_BOXSCORE_TABLE_NAME_PLACEHOLDER b
    LEFT JOIN season_statistics_T1 s1 ON b.Season = s1.Season AND b.T1_TeamID = s1.T1_TeamID
    LEFT JOIN season_statistics_T2 s2 ON b.Season = s2.Season AND b.T2_TeamID = s2.T2_TeamID
    LEFT JOIN last14days_stats_T1 l1 ON b.Season = l1.Season AND b.T1_TeamID = l1.T1_TeamID
    LEFT JOIN last14days_stats_T2 l2 ON b.Season = l2.Season AND b.T2_TeamID = l2.T2_TeamID
WHERE
    b.Season = SEASON_PLACEHOLDER AND b.DayNum = DAYNUM_PLACEHOLDER;