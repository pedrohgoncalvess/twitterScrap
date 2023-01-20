import pandas

from Main.Authentication import bearer_token
import requests
import json
from pandas import json_normalize
import pandas as pd
from pandas.core.frame import DataFrame
from typing import Dict






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

#request_content('1606775898140721152')

def scrapMessages():
    index_s = 0
    id_disc = '1053113057674534913'

    while index_s < 1000:

        headers = {
            "authorization":"NTMwNzc0OTI5NzYzOTkxNTcy.Gt_16R.AYeqdF_Wy4euNqbQy4i6_d8Yw3slnSP_xNwr2o"
        }

        url = f"https://discord.com/api/v9/channels/961719887670149181/messages?before={id_disc}"

        resp = requests.get(url,headers=headers)

        jsonn = json.loads(resp.text)

        print(jsonn)
