# Introduction

This repository is an effort to move a NCAA basketball predictional model that I have been using into a more formal and production-grade environment. It is my hope that this will allow me to more easily track progress, iterate, and improve. Much of the modeling is based off the work of Kaggle user Raddar in his [paris madness](https://www.kaggle.com/code/raddar/paris-madness/notebook) notebook.

# Project Structure
The project is organized into the following directories:

`data/`: Contains scripts for initializing and populating the PostgreSQL database with various datasets.  
`features/`: Contains SQL queries and Python scripts for transforming the data and creating training datasets.  
`models/`: Contains Python scripts for training and evaluating machine learning models, as well as making predictions on new data.  
`notebooks/`: Contains the Kaggle notebook for 2024 competition.  
`utils.py`: Utility functions for connecting to the PostgreSQL database and executing SQL queries.  

# Setup
* Create your environment with `conda` and `poetry`:
```
conda create -n liddar python=3.10
conda activate liddar
pip install poetry
poetry install
```
* Set up a PostgreSQL database and update the database.ini file with the appropriate connection details.
* Create an .env file with Kaggle credentials to pull Kaggle data

# Data Pipeline
The data pipeline consists of the following steps:
* Data Extraction: The `initialize_datasets.py` script extracts data from various sources, including the Kaggle competition dataset and the SportsDataVerse API.
* Data Transformation: The `build_features.py` script transforms the raw data into a format suitable for training machine learning models. It creates reciprocal box score tables and generates training datasets with features such as team statistics, recent performance, and game details.
* Model Training: The `train_model.py` script trains XGBoost models using the training data generated in the previous step. It performs cross-validation to optimize hyperparameters and saves the trained models to disk.
* Prediction: The `predict_model.py` script loads the trained models and generates predictions for upcoming games based on the features extracted from the schedule data.
Usage
    * Making Predictions: To generate predictions for a specific date, run python models/predict_model.py <date> <model_id>, where <date> is the date for which you want to make predictions (in the format YYYY-MM-DD), and <model_id> is the ID of the trained model you want to use (e.g., the directory name under src/models/).

# Next Steps
This was mostly an effort to get what I had in a convoluted notebook into a semi-productionalized format and to do it before the 2025 season starts. Here are the things that are top of mind for me for next steps for next season:
* Create a robust pipeline for pulling boxscores, updating training data, rerunning models as the season moves along
* Investigation into stability of team statistics and the possibility of including previous season(s) as a prior
* Integration and tracking of odds
* UI for displaying predictions, odds, and possibly tracking bets