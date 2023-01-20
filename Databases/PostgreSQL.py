import pandas as pd
import psycopg2
from psycopg2.extensions import connection
from dotenv import load_dotenv
from Main.Authentication import environment_variables
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine


load_dotenv()


def connect() -> connection:

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=environment_variables('user'),
        password=environment_variables('password'))
    conn.autocommit = True
    return conn



def engineSQL() -> Engine:
    loginDb = environment_variables('engine_sqlalchemy')
    engine = create_engine(loginDb)
    return engine


def duplicate_id(table_name:str,tweet_id:str) -> bool:
    cursor = connect().cursor()
    query = f"select * from {table_name} where idtweet = {tweet_id}"
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())
    try:
        tabela.rename(columns={0:"ID"},inplace=True)
        tabela.reset_index(inplace=True)
        tabela = tabela['ID'].iloc[0]
        id = True
    except:
        id = False

    return id
