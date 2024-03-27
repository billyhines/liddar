from dotenv import load_dotenv

from ..utils import execute_sql_query, load_config


def transform_sdv_to_kaggle():
    """
    Runs the sdv_to_kaggle_query.SQL query to get the SDV data in the same format as the Kaggle data
    """
    # Load the sdv to kaggle query
    with open("src/features/sdv_to_kaggle_query.sql", "r") as fd:
        sdv_to_kaggle_query = fd.read()

    execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=sdv_to_kaggle_query,
    )


def transform_boxscore_to_recipricol(
    boxscore_table_name, recipricol_boxscore_table_name
):
    """
    Creates the recipricol boxscores for each game boxscore

    Args:
        boxscore_table_name (str): The name of the table with boxscores.
        recipricol_boxscore_table_name (str): The name of the table where the recipricol boxscores will go.
    """
    # Load the recipricol query
    with open("src/features/swap_boxscores.sql", "r") as fd:
        recipricol_query = fd.read()

    # Parameterize the recipricol query on kaggle data
    parameterized_recipricol_query = recipricol_query.replace(
        "RECIPRICOL_BOXSCORE_TABLE_NAME_PLACEHOLDER", recipricol_boxscore_table_name
    ).replace("BOXSCORE_TABLE_NAME_PLACEHOLDER", boxscore_table_name)

    execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=parameterized_recipricol_query,
    )


def create_training_data_table(recipricol_boxscore_table_name, training_data_tablename):
    """
    Creates the training data from the recipricol boxscores

    Args:
        recipricol_boxscore_table_name (str): The name of the table with the recipricol boxscores.
        training_data_tablename (str): The name of the table where the training data will go.
    """
    # Query for all daynums and seasons within the boxscore data
    season_daynum_query = f"""
    SELECT
    Season,
    DayNum
    FROM {recipricol_boxscore_table_name}
    GROUP BY Season, DayNum
    ORDER BY Season, DayNum;
    """

    season_daynums = execute_sql_query(
        database=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        query=season_daynum_query,
    )

    # Load the training data query
    with open("src/features/create_training_data.sql", "r") as fd:
        training_data_query = fd.read()

    create_statement = f"CREATE TABLE {training_data_tablename} AS "
    insert_statement = f"INSERT INTO {training_data_tablename} "

    for i in range(0, len(season_daynums)):
        tmp_season = season_daynums[i][0]
        tmp_daynum = season_daynums[i][1]

        # replace parameters in training data query
        parameterized_training_data_query = (
            training_data_query.replace(
                "RECIPRICOL_BOXSCORE_TABLE_NAME_PLACEHOLDER",
                recipricol_boxscore_table_name,
            )
            .replace("SEASON_PLACEHOLDER", str(tmp_season))
            .replace("DAYNUM_PLACEHOLDER", str(tmp_daynum))
        )

        if i == 0:
            parameterized_training_data_query = (
                create_statement + parameterized_training_data_query
            )
        else:
            parameterized_training_data_query = (
                insert_statement + parameterized_training_data_query
            )

        execute_sql_query(
            database=config["database"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            query=parameterized_training_data_query,
        )


if __name__ == "__main__":
    # Load up configs, environment vars
    load_dotenv()
    config = load_config()

    # Transform SDV data to kaggle format
    transform_sdv_to_kaggle()

    # Create recipricol tables
    transform_boxscore_to_recipricol("boxscores_kaggle", "boxscores_kaggle_recipricol")

    transform_boxscore_to_recipricol(
        "boxscores_sdv_kagglestyle", "boxscores_sdv_kagglestyle_recipricol"
    )

    # Create training data from the kaggle dataset
    create_training_data_table("boxscores_kaggle_recipricol", "training_data_kaggle")

    # Create training data from the sdv dataset
    create_training_data_table(
        "boxscores_sdv_kagglestyle_recipricol", "training_data_sdv"
    )
