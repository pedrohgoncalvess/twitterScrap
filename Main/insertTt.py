import requests
import pandas as pd
import json
from pandas import json_normalize
from Databases.PostgreSQL import connect
from authentication import bearer_token
from pandas.core.frame import DataFrame
from typing import Dict,List
from sideFunctions import valiStatusCode as valistts



def insertNewTweet():
    cursor = connect().cursor()
    ids = listTtstandy()
    for i in ids:
        dataframe, url = requestContentTweet(i)
        dicio,url = treatmentDataTweet(dataframe,url)
        query = f"insert into tweets (idtweet,bodytt,urlimg,urltt,typemidia,impressions,likes," \
                f"retweets,rtcomment,comment) values ({dicio['idtweet']},'{dicio['text']}'," \
                f"'{url['url']}','{dicio['urltt']}','{url['type']}',{dicio['impressions']},{dicio['like']}," \
                f"{dicio['retweet']},{dicio['rtcomment']},{dicio['comment']})"
        cursor.execute(query)
        print(f"O tweet {dicio['idtweet']} foi adicionado")
    connect().close()


def listTtstandy() -> List:
    cursor = connect().cursor()
    query = "select idtweet from tweetsstandby where scraped_at is null"
    cursor.execute(query)
    table = pd.DataFrame(cursor.fetchall())
    table.rename(columns={0:"id"},inplace=True)
    listIds = []
    for i in range(len(table['id'])):
        id = table['id'].iloc[i]
        listIds.append(id)
    return listIds


def treatmentDataTweet(dataframe:DataFrame,urlimg:Dict) -> Dict | Dict:

    like = dataframe['like'].iloc[0]
    retweet = dataframe['retweet'].iloc[0]
    rtcomment = dataframe['rtcomment'].iloc[0]
    visu = dataframe['visu'].iloc[0]
    comment = dataframe['comment'].iloc[0]
    id = dataframe['id'].iloc[0]
    text = dataframe['text'].iloc[0]
    try:
        urltt = text.split('https://t.co')[1]
        urltt = 'https://t.co' + urltt
        urltt = urltt[0:23]
        text = text.split('https://t.co')[0]
    except:
        urltt = 'not-url'
        text = text

    print(urltt)
    dicio = {'like':like,'retweet':retweet,'idtweet':id,
             'text':text,'urltt':urltt,
             'rtcomment':rtcomment,'impressions':visu,
             'comment':comment}

    return dicio,urlimg


def requestContentTweet(tweet_id:str) -> DataFrame | Dict:
    headers = bearer_token()
    url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics,created_at&expansions=attachments.media_keys&media.fields=preview_image_url,url"  # IMAGE URL

    req = requests.get(url=url,headers=headers)
    vali = req.status_code
    if vali != 200:
        print(f'Numero de requisições foi excedida. Status code {vali}')
        valiStts = valistts()
        while valiStts != True:
            valiStts = valistts()
    print(tweet_id)

    data = json.loads(req.content)
    dataframe = json_normalize(data['data'])
    try:
        type = data['includes']['media'][0]['type']
        if type == 'photo':
            url = data['includes']['media'][0]['url']
            url = {'url':url,'type':'photo'}
        else:
            url = {'url': 'not-url', 'type': 'video'}
    except KeyError:
        url = {'url':'not-url','type':'comment'}
    dataframe = dataframe[['id','text','created_at','public_metrics.retweet_count',
                           'public_metrics.reply_count','public_metrics.like_count',
                           'public_metrics.quote_count','public_metrics.impression_count']]
    dataframe.rename(columns={'public_metrics.retweet_count':'retweet',
                        'public_metrics.quote_count':'rtcomment',
                        'public_metrics.like_count':'like',
                        'public_metrics.reply_count':'comment',
                        'public_metrics.impression_count':'visu'},inplace=True)
    return dataframe, url

#insertNewTweet()