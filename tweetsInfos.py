import time
from Main.Authentication import client_auth, environment_variables
import requests
import pandas as pd
from pandas import json_normalize
import json
from Databases.PostgreSQL import connect
from datetime import datetime as dt_time,timedelta
import datetime
from Main.sideFunctions import valiStatusCode


#id_user = '1512040645321531393' #Sseramemes

cursor = connect().cursor()
headers = {f"Authorization":f"Bearer {environment_variables('bearer_token')}"}#url = f"https://api.twitter.com/2/tweets?ids={id_tt}&tweet.fields=public_metrics"
#url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics&expansions=attachments.media_keys&media.fields=preview_image_url,url" #IMAGE URL


def sseramemes_tt(): #REQUEST TWEETS SSERAMEMES ACCOUNT

    client_a = client_auth()
    tweets = client_a.get_users_tweets(id='1512040645321531393',exclude=['retweets','replies'],user_auth=True).data

    for tweet in tweets:
        url = f"https://api.twitter.com/2/tweets?ids={tweet.id}&tweet.fields=public_metrics"

        id = tweet.id
        query_vali = f"select idtweet from tweetssseramemes where idtweet = {id}"
        cursor.execute(query_vali)
        vali = pd.DataFrame(cursor.fetchall())
        try:
            vali.rename(columns={0:"ID"},inplace=True)
            vali = vali['ID'].iloc[0]
            print('O tweet já está na base.')
        except:

            resp = requests.get(url=url,headers=headers)
            data = resp.content
            data = json.loads(data)
            data = json_normalize(data['data'])
            rtcomment = data['public_metrics.quote_count'].iloc[0]
            like = data['public_metrics.like_count'].iloc[0]
            rt = data['public_metrics.retweet_count'].iloc[0]
            comment = data['public_metrics.reply_count'].iloc[0]
            text = tweet.text
            id = tweet.id

            query_insert = f"INSERT INTO TWEETSSSERAMEMES (idtweet,corpott,likes,retweets,rtcomment,comment) VALUES ({id},'{text}'," \
                           f"{like},{rt},{rtcomment},{comment})"
            cursor.execute(query_insert)
            print("Insert realizado com sucesso")


def discScrap(): #GET IDS

    query = "select * from scrapDiscordMessages"
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())
    tabela.rename(columns={0:"Id"},inplace=True)
    tabela['Agrupador'] = 1
    tabela = tabela[['Id','Agrupador']].groupby('Id').count()
    tabela.reset_index(inplace=True)
    contagem = tabela['Id'].count()
    index = 0
    while index < contagem:
        tweet_id = tabela['Id'].iloc[index]
        url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics,created_at"
        url_img = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics&expansions=attachments.media_keys&media.fields=preview_image_url,url" #IMAGE URL
        query_vali = f"select idtweet from tweetssseramemes where idtweet = {tweet_id}"
        cursor.execute(query_vali)
        vali = pd.DataFrame(cursor.fetchall())
        try:
            vali.rename(columns={0:"ID"},inplace=True)
            vali = vali['ID'].iloc[0]
            print(vali)
        except:
            try:
                resp = requests.get(url=url,headers=headers)
                if resp.status_code == 429:
                    print(f"Quantidade de requisições atingiu o limite {datetime.datetime.now()}")
                    sttscode = valiStatusCode()
                    while sttscode != True:
                        sttscode = valiStatusCode()
                resp = requests.get(url=url, headers=headers)
                data = resp.content
                resp_img = requests.get(url=url_img,headers=headers)
                data_img = json.loads(resp_img.content)
                try:
                    img = data_img['includes']['media'][0]['url']
                    type_media = 'photo'
                except:
                    img = 'VIDEOURL'
                    type_media = 'video'
                data = json.loads(data)
                data = json_normalize(data['data'])
                rtcomment = data['public_metrics.quote_count'].iloc[0]
                like = data['public_metrics.like_count'].iloc[0]
                rt = data['public_metrics.retweet_count'].iloc[0]
                comment = data['public_metrics.reply_count'].iloc[0]
                date = data['created_at'].iloc[0]
                date = dt_time.strptime(date.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                date = date.strftime("%Y-%m-%d %H:%M:%S.000")
                text = data['text'].iloc[0]
                text = text.split('https')[0]
                id_tt_c = data['id'].iloc[0]
                query_insert = f"INSERT INTO TWEETSSSERAMEMES (idtweet,corpott,likes,retweets,rtcomment,comment,imgtweet,typemedia,dtcreated,dtatt) VALUES ({id_tt_c},'{text}'," \
                               f"{like},{rt},{rtcomment},{comment},'{img}','{type_media}','{date}','{dt_time.now().strftime('%Y-%m-%d %H:%M:%S.000')}')"
                cursor.execute(query_insert)
                print("Insert realizado com sucesso")
            except:
                print(resp.status_code)
                print("Tweet não existe")
        index += 1


def updateCampsTweets(): #GET IDS

    query = "select distinct(idtweet) from scrapDiscordMessages"
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())
    tabela.rename(columns={0:"Id"},inplace=True)
    tabela['Agrupador'] = 1
    tabela = tabela[['Id','Agrupador']].groupby('Id').count()
    tabela.reset_index(inplace=True)
    contagem = tabela['Id'].count()
    index = 0
    while index < contagem:
        tweet_id = tabela['Id'].iloc[index]
        url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics,created_at"
        url_img = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics&expansions=attachments.media_keys&media.fields=preview_image_url,url" #IMAGE URL
        try:
            resp = requests.get(url=url,headers=headers)
            if resp.status_code == 429:
                print(f"Quantidade de requisições atingiu o limite {datetime.datetime.now()}")
                sttscode = valiStatusCode()
                while sttscode != True:
                    sttscode = valiStatusCode()
            resp = requests.get(url=url, headers=headers)
            data = resp.content
            resp_img = requests.get(url=url_img,headers=headers)
            data_img = json.loads(resp_img.content)
            try:
                img = data_img['includes']['media'][0]['url']
                type_media = 'photo'
            except:
                img = 'VIDEOURL'
                type_media = 'video'
            data = json.loads(data)
            data = json_normalize(data['data'])
            rtcomment = data['public_metrics.quote_count'].iloc[0]
            like = data['public_metrics.like_count'].iloc[0]
            rt = data['public_metrics.retweet_count'].iloc[0]
            comment = data['public_metrics.reply_count'].iloc[0]
            date = data['created_at'].iloc[0]
            date = dt_time.strptime(date.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            date = date.strftime("%Y-%m-%d %H:%M:%S.000")
            text = data['text'].iloc[0]
            text = text.split('https')[0]
            id_tt_c = data['id'].iloc[0]
            query_insert = f"INSERT INTO TWEETSSSERAMEMES (idtweet,corpott,likes,retweets,rtcomment,comment,imgtweet,typemedia,dtcreated,dtatt) VALUES ({id_tt_c},'{text}'," \
                           f"{like},{rt},{rtcomment},{comment},'{img}','{type_media}','{date}','{dt_time.now().strftime('%Y-%m-%d %H:%M:%S.000')}')"
            cursor.execute(query_insert)
            print(f"Insert do tweet {id_tt_c} realizado com sucesso")
        except:
            print(resp.status_code)
            print("Tweet não existe")
        index += 1
    print("Terminado os inserts")



def backup_tt():
    query = "select distinct(idtweet) from tweetssseramemes"
    cursor.execute(query)
    table = pd.DataFrame(cursor.fetchall())
    table.rename(columns={0:'id'},inplace=True)
    print(table)
    tupla = table.itertuples(name=None,index=False)
    cursor.executemany('insert into backup_tt (idtweet) values (%s)',tupla)
    connect().commit()
    connect().close()
    print("Terminado a inserção")


#discScrap()