""" Requirements: 
    snowflake connector 
        pip install snowflake==0.5.0
"""

import snowflake.connector
import asyncio

# Import snowflake credentails
from credentials_file import snowflake_username, snowflake_password, snowflake_account


# Establish connectivity with Database
def connection(
    username: str,
    password: str,
    account: str,
):
    """:Connection function to a Snowflake data using Snowflake
    app login details :
    param username: str :
    param password: str :
    param account: str :
    return: connection object"""
    conn = snowflake.connector.connect(
        user=username, password=password, account=account
    )
    return conn


snowflake_connection = connection(
    snowflake_username, snowflake_password, snowflake_account
)


# fetch data as pandas dataframe
def fetch_data_into_dataframe(sql_query, connection_string):
    cur = connection_string.cursor()
    cur.execute(sql_query)
    df = cur.fetch_pandas_all()
    return df


sql_query1 = """SELECT * FROM "DB"."SCHEMA".TABLE1" """
sql_query2 = """SELECT * FROM "DB"."SCHEMA".TABLE2" """


# Define asynchronous function to fetch data
async def fetch_data_into_dataframe_async(sql_query: str, connection_string: str):
    """Helper function to make fetch_data_into_dataframe an async function. This
    will start fetching the query and allows computer to process multiples queries or
    other computation while it waits
    Args:
    sql_query (str): sql query string
    conn (connection string): database connection

    retuns:
    event loop: event loop of all queries
    """
    return fetch_data_into_dataframe(sql_query, connection_string)


async def main():
    """
    Run asynchronous tasks concurrently and
    gather all the results from the async functions called

    Returns: pd.DataFrame
    """
    tasks = [
        fetch_data_into_dataframe_async(sql_query1, snowflake_connection),
        fetch_data_into_dataframe_async(sql_query2, snowflake_connection),
    ]
    results = await asyncio.gather(*tasks)
    # Results will contain pandas dataframes and return dataframes in the order of task schedule
    df1, df2 = results[0], results[1]

    print(df1.head())
    print(df.head())


if __name__ == "__main__":
    asyncio.run(main())
