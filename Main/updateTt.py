from authentication import client_auth,bearer_token
from sideFunctions import timeBaser, valiStatusCode
from Databases.PostgreSQL import connect

import requests
import pandas as pd
import json
from datetime import datetime as dt_time

client_a = client_auth()
cursor = connect().cursor()



def updateTweet():
    print(f"Atualizando tweets a partir de {timeBaser()}")
    headers = bearer_token()

    query = f"select idtweet from tweetssseramemes where dtcreated > '{timeBaser()}'"
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())

    tabela.rename(columns={0:'ID'},inplace=True)
    contagem = tabela['ID'].count()

    index_s = 0

    while index_s < contagem:
        tweet_id = tabela['ID'].iloc[index_s]
        url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics,created_at"
        req = requests.get(url=url, headers=headers)
        status = req.status_code
        if status == 200:
            tweet_id = tabela['ID'].iloc[index_s]
            data = json.loads(req.text)
            try:
                data = data['data'][0]
            except:
                query_delete = f"delete from tweetssseramemes where idtweet = {tweet_id} "
                print(f"O tweet {tweet_id} não existe e foi excluido do DB.")
                cursor.execute(query_delete)
                index_s += 1
                continue
            rt = data['public_metrics']['retweet_count']
            comment = data['public_metrics']['reply_count']
            like = data['public_metrics']['like_count']
            quote = data['public_metrics']['quote_count']
            date = dt_time.strptime(data['created_at'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            date = date.strftime("%Y-%m-%d %H:%M:%S.000")

            query_update = f"UPDATE tweetssseramemes set likes = {like},retweets = {rt}, rtcomment = {quote}, " \
                           f"comment = {comment}," \
                           f"dtatt = '{dt_time.now().strftime('%Y-%m-%d %H:%M:%S.000')}', dtcreated = '{date}'  where idtweet = {tweet_id}"
            cursor.execute(query_update)
            print(f"Update realizado com sucesso. ID:{tweet_id}")
        elif status == 429:
            print(index_s)
            check = valiStatusCode()
            while check != True:
                check = valiStatusCode()
                index_s -= 1
        else:
            print(f"Não foi possivel fazer o insert do tweet com id {tweet_id}.")
            print(tweet_id)
            print(status)
            print(dt_time.now())
        index_s += 1


