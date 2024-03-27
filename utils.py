from configparser import ConfigParser
import pandas as pd
import psycopg2
import re


def load_config(filename="database.ini", section="postgresql"):
    """
    Load database configuration from an INI file.

    Args:
        filename (str): The path to the INI file containing the database configuration.
        section (str): The name of the section in the INI file containing the database configuration.

    Returns:
        dict: A dictionary containing the database configuration parameters.
    """

    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, filename)
        )

    return config


def create_table(database, user, password, host, port, table_name, table_definition):
    """
    Creates a table in the specified PostgreSQL database.

    Args:
        database (str): The name of the database.
        user (str): The database user.
        password (str): The database user's password.
        host (str): The database host address.
        port (str): The database port.
        table_name (str): The name of the table to create.
        table_definition (str): The SQL CREATE TABLE statement defining the table's columns.
    """

    try:
        # Connect to the database
        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )

        # Create a cursor object
        cur = conn.cursor()

        # Execute the CREATE TABLE statement
        cur.execute(table_definition)

        # Commit the changes
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while creating PostgreSQL table", error)

    finally:
        # Close the database connection
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


def insert_dataframe(df, table_name, database_config):
    """
    Insert a pandas DataFrame into a PostgreSQL table.

    Args:
        df (pandas.DataFrame): The DataFrame to be inserted into the table.
        table_name (str): The name of the table to insert the data into.
        database_config (dict): A dictionary containing the database configuration parameters.
    """
    conn = None
    try:
        conn = psycopg2.connect(**database_config)
        cur = conn.cursor()

        # Create insert template
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        # Insert each row
        for _, row in df.iterrows():
            cur.execute(sql, tuple(row))

        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def execute_sql_query(database, user, password, host, port, query, return_pandas=False):
    """
    Executes a SQL query and returns the results if there are any.
    If the query modifies the database (e.g., CREATE TABLE, INSERT INTO, UPDATE, DELETE),
    it performs the query and returns an empty list.

    Args:
        database (str): The name of the database.
        user (str): The database user.
        password (str): The database user's password.
        host (str): The database host address.
        port (str): The database port.
        query (str): The SQL query to execute.
        return_pandas (bool): If True, the results are returned in a pandas dataframe.

    Returns:
        list: A list of tuples, where each tuple represents a row of results.
              An empty list is returned if the query modifies the database or if an error occurs.
    """
    modify_database_regex = re.compile(r"^(CREATE|INSERT|UPDATE|DELETE)", re.IGNORECASE)

    try:
        # Connect to the database
        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
        # Create a cursor object
        cur = conn.cursor()
        # Execute the SQL query
        cur.execute(query)

        # Check if the query modifies the database
        if modify_database_regex.match(query):
            # Commit the changes
            conn.commit()
            results = []
        else:
            # Fetch the results
            results = cur.fetchall()
            if return_pandas:
                columns = [desc[0] for desc in cur.description]
                results = pd.DataFrame(results, columns=columns)

    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)
        results = []

    finally:
        # Close the database connection
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

    return results
