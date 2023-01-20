import time
import requests
import pandas as pd
import json
from pandas import DataFrame
from pandas import json_normalize
from Databases.PostgreSQL import connect
from datetime import timedelta
import datetime
from Main.Authentication import bearer_token
from pandas.core.frame import DataFrame
from typing import Dict,List



def insertNewTweet():
    ids = listTtstandy()
    for i in ids:
        dataframe,url = requestContentTweet(i)
        like = dataframe['like'].iloc[0]
        retweet = dataframe['retweet'].iloc[0]
        rtcomment = dataframe['rtcomment'].iloc[0]
        visu = dataframe['visu'].iloc[0]
        comment = dataframe['comment'].iloc[0]
        id = dataframe['id'].iloc[0]
        text = dataframe['text'].iloc[0]
        #query = f"update into tweets set idtweet={id}, likes={like},"
        print(text)




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




def requestContentTweet(tweet_id:str) -> DataFrame | Dict:
    headers = bearer_token()
    url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields=public_metrics,created_at&expansions=attachments.media_keys&media.fields=preview_image_url,url"  # IMAGE URL

    req = requests.get(url=url,headers=headers)
    data = json.loads(req.content)
    dataframe = json_normalize(data['data'])
    type = data['includes']['media'][0]['type']
    if type == 'photo':
        url = data['includes']['media'][0]['url']
        url = {'url':url,'type':'photo'}
    else:
        url = {'url': 'videourl', 'type': 'video'}
    dataframe = dataframe[['id','text','created_at','public_metrics.retweet_count',
                           'public_metrics.reply_count','public_metrics.like_count',
                           'public_metrics.quote_count','public_metrics.impression_count']]
    dataframe.rename(columns={'public_metrics.retweet_count':'retweet',
                        'public_metrics.quote_count':'rtcomment',
                        'public_metrics.like_count':'like',
                        'public_metrics.reply_count':'comment',
                        'public_metrics.impression_count':'visu'},inplace=True)
    return dataframe, url

insertNewTweet()