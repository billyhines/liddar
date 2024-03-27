from dotenv import load_dotenv
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs
import sportsdataverse
from zipfile import ZipFile

from ..utils import create_table, insert_dataframe, load_config


def create_kaggle_boxscore_table(config):
    """
    Create a table named 'boxscores_kaggle' in the PostgreSQL database.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    table_name = "boxscores_kaggle"
    table_definition = """
        CREATE TABLE boxscores_kaggle (
            Season INTEGER,
            DayNum INTEGER,
            WTeamID INTEGER,
            WScore INTEGER,
            LTeamID INTEGER,
            LScore INTEGER,
            WLoc VARCHAR,
            NumOT INTEGER,
            WFGM INTEGER,
            WFGA INTEGER,
            WFGM3 INTEGER,
            WFGA3 INTEGER,
            WFTM INTEGER,
            WFTA INTEGER,
            WOR INTEGER,
            WDR INTEGER,
            WAst INTEGER,
            WTO INTEGER,
            WStl INTEGER,
            WBlk INTEGER,
            WPF INTEGER,
            LFGM INTEGER,
            LFGA INTEGER,
            LFGM3 INTEGER,
            LFGA3 INTEGER,
            LFTM INTEGER,
            LFTA INTEGER,
            LOR INTEGER,
            LDR INTEGER,
            LAst INTEGER,
            LTO INTEGER,
            LStl INTEGER,
            LBlk INTEGER,
            LPF INTEGER)
    """
    create_table(**config, table_name=table_name, table_definition=table_definition)


def get_and_populate_kaggle_data(config):
    """
    Download and populate the 'boxscores_kaggle' table with data from the Kaggle competition.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    api.competition_download_files(
        "march-machine-learning-mania-2024", path="data/external"
    )

    with ZipFile("data/external/march-machine-learning-mania-2024.zip", "r") as zObject:
        zObject.extractall(path="data/external/march-machine-learning-mania-2024")

    df = pd.read_csv(
        "data/external/march-machine-learning-mania-2024/MRegularSeasonDetailedResults.csv"
    )
    table_name = "boxscores_kaggle"
    insert_dataframe(df, table_name, config)


def create_sdv_boxscore_table(config):
    """
    Create a table named 'boxscores_sdv' in the PostgreSQL database.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    table_name = "boxscores_sdv"
    table_definition = """
        CREATE TABLE boxscores_sdv (
            game_id INTEGER,
            season INTEGER,
            season_type INTEGER,
            game_date DATE,
            game_date_time TIMESTAMP,
            team_id INTEGER,
            team_uid VARCHAR,
            team_slug VARCHAR,
            team_location VARCHAR,
            team_name VARCHAR,
            team_abbreviation VARCHAR,
            team_display_name VARCHAR,
            team_short_display_name VARCHAR,
            team_color VARCHAR,
            team_alternate_color VARCHAR,
            team_logo VARCHAR,
            team_home_away VARCHAR,
            team_score INTEGER,
            team_winner BOOLEAN,
            assists INTEGER,
            blocks INTEGER,
            defensive_rebounds INTEGER,
            fast_break_points INTEGER,
            field_goal_pct DOUBLE PRECISION,
            field_goals_made INTEGER,
            field_goals_attempted INTEGER,
            flagrant_fouls INTEGER,
            fouls INTEGER,
            free_throw_pct DOUBLE PRECISION,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            largest_lead VARCHAR,
            offensive_rebounds INTEGER,
            points_in_paint INTEGER,
            steals INTEGER,
            team_turnovers INTEGER,
            technical_fouls INTEGER,
            three_point_field_goal_pct DOUBLE PRECISION,
            three_point_field_goals_made INTEGER,
            three_point_field_goals_attempted INTEGER,
            total_rebounds INTEGER,
            total_technical_fouls INTEGER,
            total_turnovers INTEGER,
            turnover_points INTEGER,
            turnovers INTEGER,
            opponent_team_id INTEGER,
            opponent_team_uid VARCHAR,
            opponent_team_slug VARCHAR,
            opponent_team_location VARCHAR,
            opponent_team_name VARCHAR,
            opponent_team_abbreviation VARCHAR,
            opponent_team_display_name VARCHAR,
            opponent_team_short_display_name VARCHAR,
            opponent_team_color VARCHAR,
            opponent_team_alternate_color VARCHAR,
            opponent_team_logo VARCHAR,
            opponent_team_score INTEGER)
    """
    create_table(**config, table_name=table_name, table_definition=table_definition)


def get_and_populate_sdv_data(seasons, config):
    """
    Populate the 'boxscores_sdv' table with data from the SportsDataVerse API.

    Args:
        seasons (list): A list of seasons for which to retrieve data.
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    pre_24_seasons = [x for x in seasons if x < 2024]
    post_24_seasons = [x for x in seasons if x >= 2024]

    # pre 2024
    pre_mbb_df = sportsdataverse.mbb.load_mbb_team_boxscore(
        seasons=pre_24_seasons, return_as_pandas=True
    )
    pre_mbb_df["fast_break_points"] = None
    pre_mbb_df["points_in_paint"] = None
    pre_mbb_df["turnover_points"] = None

    # post 2024
    post_mbb_df = sportsdataverse.mbb.load_mbb_team_boxscore(
        seasons=post_24_seasons, return_as_pandas=True
    )

    sdv_df = pd.concat([pre_mbb_df, post_mbb_df])

    table_name = "boxscores_sdv"
    insert_dataframe(sdv_df, table_name, config)


def create_training_run_table(config):
    """
    Create a table named 'training_runs' in the PostgreSQL database.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    table_name = "training_runs"
    table_definition = f"""
        CREATE TABLE {table_name} (
            trainingTimestamp TIMESTAMP,
            fileLocation VARCHAR,
            iterationCounts INTEGER,
            valMae DOUBLE PRECISION,
            trainingExamples INTEGER)
    """
    create_table(**config, table_name=table_name, table_definition=table_definition)


def create_sdv_schedule_table(config):
    """
    Create a table named 'schedule_sdv' in the PostgreSQL database.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    table_name = "schedule_sdv"
    table_definition = f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            uid TEXT,  
            date TEXT,
            attendance DOUBLE PRECISION, 
            time_valid BOOLEAN,
            neutral_site BOOLEAN,
            conference_competition BOOLEAN,
            play_by_play_available BOOLEAN,
            recent BOOLEAN,
            start_date TIMESTAMP WITH TIME ZONE, -- For UTC timestamps
            notes_type TEXT, 
            notes_headline TEXT, 
            broadcast_market TEXT, 
            broadcast_name TEXT,  
            type_id INTEGER,
            type_abbreviation TEXT, 
            venue_id INTEGER, 
            venue_full_name TEXT,
            venue_address_city TEXT, 
            venue_address_state TEXT,
            venue_capacity DOUBLE PRECISION, 
            venue_indoor BOOLEAN,  
            status_clock DOUBLE PRECISION,
            status_display_clock TEXT,   
            status_period DOUBLE PRECISION, 
            status_type_id INTEGER,  
            status_type_name TEXT,  
            status_type_state TEXT,
            status_type_completed BOOLEAN,  
            status_type_description TEXT, 
            status_type_detail TEXT, 
            status_type_short_detail TEXT,  
            format_regulation_periods DOUBLE PRECISION, 
            home_id INTEGER,  
            home_uid TEXT,
            home_location TEXT, 
            home_name TEXT,  
            home_abbreviation TEXT,
            home_display_name TEXT, 
            home_short_display_name TEXT, 
            home_color TEXT,  
            home_alternate_color TEXT,  
            home_is_active BOOLEAN,  
            home_venue_id INTEGER,  
            home_logo TEXT,  
            home_conference_id INTEGER,
            home_score INTEGER, 
            home_winner BOOLEAN,
            home_current_rank DOUBLE PRECISION,  
            home_linescores TEXT, 
            home_records TEXT, 
            away_id INTEGER, 
            away_uid TEXT, 
            away_location TEXT,  
            away_name TEXT,  
            away_abbreviation TEXT,  
            away_display_name TEXT,
            away_short_display_name TEXT,
            away_color TEXT,  
            away_alternate_color TEXT,
            away_is_active BOOLEAN,
            away_venue_id INTEGER,
            away_logo TEXT,
            away_conference_id INTEGER,
            away_score INTEGER,
            away_winner BOOLEAN,
            away_current_rank DOUBLE PRECISION,
            away_linescores TEXT,
            away_records TEXT,
            game_id INTEGER,  
            season INTEGER, 
            season_type INTEGER, 
            status_type_alt_detail TEXT,  
            tournament_id INTEGER, 
            groups_id INTEGER, 
            groups_name TEXT, 
            groups_short_name TEXT,  
            groups_is_conference BOOLEAN,
            game_json BOOLEAN,  
            game_json_url TEXT, 
            game_date_time TIMESTAMP WITH TIME ZONE, -- For UTC timestamps  
            game_date DATE, 
            pbp BOOLEAN,  
            team_box BOOLEAN,  
            player_box BOOLEAN,
            season_start_date TIMESTAMP WITH TIME ZONE, -- For UTC timestamps 
            daynum BIGINT)
    """
    create_table(**config, table_name=table_name, table_definition=table_definition)


def get_and_populate_sdv_schedule_data(seasons, config):
    """
    Populate the 'schedule_sdv' table with schedule data from the SportsDataVerse API.

    Args:
        seasons (list): A list of seasons for which to retrieve schedule data.
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """

    # TODO: fix for pre-2024
    sched_df = sportsdataverse.mbb.load_mbb_schedule(
        seasons=seasons, return_as_pandas=True
    )

    # Caluclate a DayNum for the season
    sched_df["start_date"] = pd.to_datetime(sched_df["start_date"])
    season_start_dates = (
        sched_df.groupby("season")["start_date"].min().rename("season_start_date")
    )
    sched_df = sched_df.merge(
        season_start_dates, how="left", left_on="season", right_index=True
    )
    sched_df["DayNum"] = (
        sched_df["start_date"] - sched_df["season_start_date"]
    ).dt.days

    table_name = "schedule_sdv"
    insert_dataframe(sched_df, table_name, config)

def create_predictions_table(config):
    """
    Create a table named 'predictions' in the PostgreSQL database.

    Args:
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.
    """
    table_name = "predictions"
    table_definition = f"""
        CREATE TABLE {table_name} (
            t1_teamid INTEGER,
            t2_teamid INTEGER,
            pred_spread DOUBLE PRECISION,
            season INTEGER,
            daynum INTEGER,
            id INTEGER,
            game_id INTEGER,
            home_display_name TEXT,
            away_display_name TEXT)
    """
    create_table(**config, table_name=table_name, table_definition=table_definition)    


if __name__ == "__main__":
    # Load up configs, environment vars
    load_dotenv()
    config = load_config()

    # Adapt pandas NAs to postgres NULLs
    register_adapter(pd._libs.missing.NAType, lambda i: AsIs("NULL"))

    # Kaggle dataset
    create_kaggle_boxscore_table(config)
    get_and_populate_kaggle_data(config)

    # SDV dataset
    create_sdv_boxscore_table(config)
    seasons = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    get_and_populate_sdv_data(seasons, config)

    # SDV schedule
    create_sdv_schedule_table(config)
    seasons = [2024]
    get_and_populate_sdv_schedule_data(seasons, config)

    # Initialize table for training runs
    create_training_run_table(config)

    # Initialize table for predictions
    create_predictions_table(config)
