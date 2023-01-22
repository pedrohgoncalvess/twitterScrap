from Databases.PostgreSQL import engineSQL
from sideFunctions import valiStatusCode
from authentication import bearer_token

import requests
from pandas import DataFrame
from pandas import json_normalize
import pandas as pd
import json
from datetime import datetime,timedelta
from psycopg2.errorcodes import UNIQUE_VIOLATION


def tweets(haveTries:str,daysReduce:int = 2):
    inicio = datetime.today() - timedelta(days=daysReduce)
    final = datetime.today()
    index = 0
    tries = 0
    while index <= 120:

        srt_final = final.strftime("%Y-%m-%dT%H:00:00-00:00")
        srt_inicio = inicio.strftime("%Y-%m-%dT%H:00:00-00:00")
        srt_final = srt_final.replace(":", "%3A")
        srt_inicio = srt_inicio.replace(":", "%3A")

        dataframe, resp = requestScrapD2D(srt_inicio,srt_final)
        if resp != 200:
            inicio = inicio - timedelta(days=daysReduce)
            final = final - timedelta(days=daysReduce)
            index += 1
            continue
        else:
            for i in range(len(dataframe)):
                try:
                    dataframe.iloc[i:i + 1].to_sql(name='tweetsstandby', index=False, con=engineSQL(), if_exists='append',method=None)
                    print(f"Insert do tweet {dataframe['idtweet'].iloc[i]} realizado com sucesso.")
                except UNIQUE_VIOLATION:
                    print("O tweet já existe na base de dados.")
                    tries += 1
                    if haveTries.upper() == 'Y':
                        if tries > 15:
                            print("Foi renovada toda a base de dados.")
                            exit()
                    else:
                        pass

        inicio = inicio - timedelta(days=daysReduce)
        final = final - timedelta(days=daysReduce)

        index += 1
        print(f"Dia baseador {inicio}")
    print("Foi renovada toda a base de dados.")




def requestScrapD2D(srt_inicio:str,srt_final:str) -> DataFrame | int:
    headers = bearer_token()
    url = f"https://api.twitter.com/2/users/1512040645321531393/tweets?max_results=100&end_time=" \
          f"{srt_final}&start_time={srt_inicio}" \
          f"&exclude=retweets&tweet.fields=public_metrics,created_at"
    request = requests.get(url=url,headers=headers)
    data = request.content
    if request.status_code == 420:
        vali = valiStatusCode()
        while vali != True:
            vali = valiStatusCode()
    try:
        dataframe = json.loads(data)
        dataframe = json_normalize(dataframe['data'])
        dataframe = dataframe[['id','created_at']]
        dataframe.rename(columns={'id':'idtweet',
                                  'created_at':'dtcreated'},inplace=True)
        resp = 200
    except:
        dataframe = pd.DataFrame()
        resp = 400
    return dataframe, resp