from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
from sklearn.model_selection import KFold
import xgboost as xgb

from ..utils import execute_sql_query, insert_dataframe, load_config


def load_training_data(config):
    """
    Load training data from the training_data_kaggle table.

    Args:
        config (dict): A dictionary containing database connection parameters.

    Returns:
        pandas.DataFrame: A DataFrame containing the loaded training data.
    """
    training_data_query = """ 
        SELECT
        * 
        FROM training_data_kaggle 
        WHERE DayNum >= 90;
    """
    training_data = execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=training_data_query,
        return_pandas=True,
    )
    return training_data


def preprocess_data(training_data):
    """
    Preprocess training data for XGBoost training.

    Args:
        training_data (pandas.DataFrame): A DataFrame containing training data.

    Returns:
        tuple: A tuple containing features (X) and target variable (y).
    """
    y = training_data["t1_score"] - training_data["t2_score"]

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

    X = training_data[features].values
    return X, y


def cauchyobj(preds, dtrain):
    """
    Custom objective function for XGBoost.

    Args:
        preds (numpy.ndarray): Predictions made by the model.
        dtrain (xgb.DMatrix): The training data.

    Returns:
        tuple: A tuple containing gradient and hessian.
    """
    labels = dtrain.get_label()
    c = 5000
    x = preds - labels
    grad = x / (x**2 / c**2 + 1)
    hess = -(c**2) * (x**2 - c**2) / (x**2 + c**2) ** 2
    return grad, hess


def train_and_evaluate_models(X, y, param, repeat_cv=3):
    """
    Train and evaluate XGBoost models using cross-validation.

    Args:
        X (numpy.ndarray): Features for training.
        y (numpy.ndarray): Target variable for training.
        param (dict): Parameters for XGBoost model.
        repeat_cv (int, optional): Number of times to repeat cross-validation. Default is 3.

    Returns:
        tuple: A tuple containing iteration counts and validation mean absolute errors.
    """
    dtrain = xgb.DMatrix(X, label=y)
    xgb_cv = []
    for i in range(repeat_cv):
        print(f"Fold repeater {i}")
        xgb_cv.append(
            xgb.cv(
                params=param,
                dtrain=dtrain,
                obj=cauchyobj,
                num_boost_round=800,
                folds=KFold(n_splits=5, shuffle=True, random_state=i),
                early_stopping_rounds=25,
                verbose_eval=50,
            )
        )
    iteration_counts = [np.argmin(x["test-mae-mean"].values) for x in xgb_cv]
    val_mae = [np.min(x["test-mae-mean"].values) for x in xgb_cv]
    return iteration_counts, val_mae


def train_and_save_models(X, y, param, iteration_counts, new_folder_path, repeat_cv=3):
    """
    Train XGBoost models and save them to a specified folder.

    Args:
        X (numpy.ndarray): Features for training.
        y (numpy.ndarray): Target variable for training.
        param (dict): Parameters for XGBoost model.
        iteration_counts (list): Iteration counts obtained from cross-validation.
        new_folder_path (str): Path to the folder where models will be saved.
        repeat_cv (int, optional): Number of times to repeat cross-validation. Default is 3.

    Returns:
        pandas.DataFrame: DataFrame containing information about the training run.
    """
    dtrain = xgb.DMatrix(X, label=y)

    training_run_data = []

    pred_models = []
    for i in range(repeat_cv):
        print(f"Fold repeater {i}")
        pred_models.append(
            xgb.train(
                params=param,
                dtrain=dtrain,
                num_boost_round=int(iteration_counts[i] * 1.05),
                verbose_eval=50,
            )
        )
        filename = f"xgboost_model_{datetime_str}_{str(i)}.model"
        pred_models[i].save_model(os.path.join(new_folder_path, filename))

        training_run_data.append(
            {
                "trainingTimestamp": now,
                "fileLocation": new_folder_path + "/" + filename,
                "iterationCounts": int(iteration_counts[i] * 1.05),
                "valMae": val_mae[i],
                "trainingExamples": len(X),
            }
        )

    training_run_data = pd.DataFrame(training_run_data)
    return training_run_data


if __name__ == "__main__":
    # Load up configs, environment vars
    load_dotenv()
    config = load_config()

    # Load training data
    training_data = load_training_data(config)

    # Seperate observations and results
    X, y = preprocess_data(training_data)

    # Define parameters
    param = {}
    param["eval_metric"] = "mae"
    param["booster"] = "gbtree"
    param["eta"] = 0.05
    param["subsample"] = 0.35
    param["colsample_bytree"] = 0.7
    param["num_parallel_tree"] = 3
    param["min_child_weight"] = 40
    param["gamma"] = 10
    param["max_depth"] = 3

    iteration_counts, val_mae = train_and_evaluate_models(X, y, param)

    # Create a new folder for saving models
    now = datetime.now()
    datetime_str = now.strftime("%Y%m%d%H%M%S")
    new_folder_path = f"src/models/{datetime_str}"
    os.mkdir(new_folder_path)

    # Train and save final models
    training_run_data = train_and_save_models(
        X, y, param, iteration_counts, new_folder_path
    )

    # Save training run data
    insert_dataframe(training_run_data, "training_runs", config)
