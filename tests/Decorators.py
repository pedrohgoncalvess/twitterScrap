from psycopg2.extensions import cursor
from types import FunctionType


def insert(func:FunctionType):
    from Databases.PostgreSQL import connect
    cursor = connect().cursor()
    def executa_query():
        func(cursor)
        connect().commit()
        connect().close()
    return executa_query


@insert
def updateTweet(cursor:cursor):
    query = "insert into teste values ('pedro',20)"
    return cursor.execute(query)


@insert
def updateTweetDois(cursor:cursor):
    query = "insert into teste values ('emilly',19)"
    return cursor.execute(query)

insert(updateTweetDois())