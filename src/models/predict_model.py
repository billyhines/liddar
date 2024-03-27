import argparse
import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import xgboost as xgb

from ..utils import execute_sql_query, insert_dataframe, load_config


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.

    This function initializes an ArgumentParser object and defines two required
    command-line arguments: 'date' and 'model_id'.
    """
    parser = argparse.ArgumentParser(description="Run the predict_model script")
    parser.add_argument("date", type=str, help="The date to use for prediction")
    parser.add_argument(
        "model_id", type=str, help="The ID of the model to use for prediction"
    )
    return parser.parse_args()


def get_scheduled_games(date, config):
    """
    Retrieve scheduled games for a given date from the schedule_sdv table.

    Args:
        date (str or datetime.date): The date for which to retrieve scheduled games.
            It can be either a string in the format 'YYYY-MM-DD' or a datetime.date object.
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.

    Returns:
        pandas.DataFrame: A DataFrame containing information about the scheduled games.
    """

    # Ensure date is a datetime.date object
    if not isinstance(date, datetime.date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    # Format the date string
    formatted_date = date.strftime("%Y-%m-%d")

    schedule_query = f"""
    SELECT
    *
    FROM schedule_sdv
    WHERE start_date::DATE = '{formatted_date}' 
    """

    schedule_games = execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=schedule_query,
        return_pandas=True,
    )

    schedule_games["location"] = schedule_games["neutral_site"].apply(
        lambda x: 0 if x else 1
    )

    schedule_games = schedule_games[
        [
            "season",
            "daynum",
            "id",
            "home_id",
            "away_id",
            "game_id",
            "home_short_display_name",
            "away_short_display_name",
            "home_display_name",
            "away_display_name",
            "location",
        ]
    ].rename(
        columns={
            "home_id": "t1_teamid",
            "away_id": "t2_teamid",
        }
    )

    return schedule_games


# Get features
def get_game_features(games, config):
    """
    Retrieve features for the provided games from a database.

    Args:
        games (pandas.DataFrame): A DataFrame containing information about the games.
        config (dict): A dictionary containing the PostgreSQL database configuration parameters.

    Returns:
        numpy.ndarray: An array containing the extracted features for the games.
    """

    # Get the daynum and season of the games
    DayNum = games["daynum"].values[0]
    season = games["season"].values[0]

    # Load the feature creation query
    with open("src/models/get_team_features.sql", "r") as fd:
        team_features_query = fd.read()

    # Parameterize the feature creation query
    parameterized_team_features_query = team_features_query.replace(
        "SEASON_PLACEHOLDER", str(season)
    ).replace("DAYNUM_PLACEHOLDER", str(DayNum))

    team_features = execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=parameterized_team_features_query,
        return_pandas=True,
    )

    # Merge team features back onto the games
    games = games.merge(team_features, how="left", on="t1_teamid")
    T2_cols = {
        col: col.replace("t1_", "t2_")
        for col in team_features.columns
        if col.startswith("t1_")
    }
    T2_team_features = team_features.rename(columns=T2_cols)
    games = games.merge(T2_team_features, how="left", on="t2_teamid")

    # Return the features back
    features = [
        "T1_FGMmean",
        "T1_FGAmean",
        "T1_FGM3mean",
        "T1_FGA3mean",
        "T1_ORmean",
        "T1_Astmean",
        "T1_TOmean",
        "T1_Stlmean",
        "T1_PFmean",
        "T1_opponent_FGMmean",
        "T1_opponent_FGAmean",
        "T1_opponent_FGM3mean",
        "T1_opponent_FGA3mean",
        "T1_opponent_ORmean",
        "T1_opponent_Astmean",
        "T1_opponent_TOmean",
        "T1_opponent_Stlmean",
        "T1_opponent_Blkmean",
        "T1_PointDiffmean",
        "T2_FGMmean",
        "T2_FGAmean",
        "T2_FGM3mean",
        "T2_FGA3mean",
        "T2_ORmean",
        "T2_Astmean",
        "T2_TOmean",
        "T2_Stlmean",
        "T2_PFmean",
        "T2_opponent_FGMmean",
        "T2_opponent_FGAmean",
        "T2_opponent_FGM3mean",
        "T2_opponent_FGA3mean",
        "T2_opponent_ORmean",
        "T2_opponent_Astmean",
        "T2_opponent_TOmean",
        "T2_opponent_Stlmean",
        "T2_opponent_Blkmean",
        "T2_PointDiffmean",
        "T1_win_ratio_14d",
        "T2_win_ratio_14d",
        "DayNum",
        "location",
    ]

    features = [f.lower() for f in features]

    X = games[features].values

    return X


def load_models(model_id):
    """
    Load XGBoost models from the specified directory for a given model ID.

    Args:
        model_id (str): The ID of the model to load.

    Returns:
        list: A list containing the loaded XGBoost models.
    """
    model_dir = f"src/models/{model_id}/"

    models = []
    for i in range(3):
        filename = f"xgboost_model_{model_id}_{str(i)}.model"
        model_path = os.path.join(model_dir, filename)
        model = xgb.Booster(model_file=model_path)
        models.append(model)
    return models


def generate_predictions(model_id, X):
    """
    Generate predictions using XGBoost models for the provided data.

    Args:
        model_id (str): The ID of the XGBoost model to use for predictions.
        X (numpy.ndarray): An array containing the input data for making predictions.

    Returns:
        pandas.Series: A Series containing the mean predictions generated by the models.
    """
    models = load_models(model_id)
    dtest = xgb.DMatrix(X)

    preds = []
    for i in range(3):
        preds.append(models[i].predict(dtest))
    mean_predctions = pd.DataFrame(preds).mean(axis=0)
    mean_predctions.name = "pred_spread"

    return mean_predctions


if __name__ == "__main__":
    # Load up configs, environment vars, args
    load_dotenv()
    config = load_config()
    args = parse_arguments()

    # Load game data
    games = get_scheduled_games(args.date, config)

    # Create features for games
    X = get_game_features(games, config)

    # Load up models and generate predictions
    predictions = generate_predictions(args.model_id, X)

    game_predictions = games[["t1_teamid", "t2_teamid"]].merge(
        predictions, how="left", left_index=True, right_index=True
    )
    game_predictions = game_predictions.merge(
        games[
            [
                "season",
                "daynum",
                "id",
                "t1_teamid",
                "t2_teamid",
                "game_id",
                "home_display_name",
                "away_display_name",
            ]
        ],
        how="left",
        on=["t1_teamid", "t2_teamid"],
    )

    insert_dataframe(game_predictions, "predictions", config)
