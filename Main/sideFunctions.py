import time
import requests
import pandas as pd
from datetime import timedelta
import datetime
from pandas import DataFrame

from Databases.PostgreSQL import connect
from authentication import bearer_token
from Logs.logModule import writeLog


def timeBaser() -> str:
    hoje = datetime.datetime.now() - timedelta(days=20)
    hoje = hoje.strftime('%d/%m/%Y')
    return hoje


def valiImgUrl(id:int,nameColumn:str) -> DataFrame:
    cursor = connect().cursor()

    query = f"select {nameColumn} from tweetssseramemes where idtweet = {id}"
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())
    tabela.rename(columns={0:'IMG_TWEET'},inplace=True)
    situ = tabela['IMG_TWEET'].iloc[0]

    return situ


def valiDupli(idtweet:int) -> bool:
    cursor = connect().cursor()
    try:
        query = f"select idtweet from tweetssseramemes where idtweet = {idtweet}"
        cursor.execute(query)
        tabela = pd.DataFrame(cursor.fetchall())
        tabela.rename(columns={0:'valiDupli'},inplace=True)
        try:
            situ = tabela['valiDupli'].iloc[0]
            situ = True
        except:
            situ = False
    except:
        situ = False
        return situ

    return situ


def valiStatusCode() -> bool:
    headers = bearer_token()
    url = f"https://api.twitter.com/2/tweets?ids=1603554320506650624&tweet.fields=public_metrics,created_at"
    req = requests.get(url=url,headers=headers)

    while req.status_code != 200:
        time.sleep(240)
        req = requests.get(url=url, headers=headers)
        dictLogRequest = {'event':'Request','level':''}


    requisicao = True
    return requisicao


