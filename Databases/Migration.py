from Databases.PostgreSQL import connect
import pandas as pd
from datetime import datetime
from pandas import NaT


def migration():
    cursor = connect().cursor()
    query = 'select * from tweetssseramemes'
    cursor.execute(query)
    tabela = pd.DataFrame(cursor.fetchall())
    tabela.rename(columns={0:'_id',
                           1:'corpott',
                           2:'likes',
                           3:'retweets',
                           4:'rtcomment',
                           5:'comment',
                           6:'urlimg',
                           7:'mediatype',
                           8:'dtcreated',
                           9:'dtatt'},inplace=True)
    dict_table = tabela.to_dict(orient='records')
    keys = ['dtatt','dtcreated']
    for tweet in dict_table:
        print(tweet)
        for key in keys:
            value = tweet[key]
            if value == NaT:
                del tweet[key]
            elif value == None:
                del tweet[key]
                #tweetsConnection().insert_one(tweet)
                print(tweet)
            else:
                value = str(value)
                value = value.replace('Timestamp','')
                value = value.replace('(','')
                value = value.replace(')', '')
                value = datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
                del tweet[key]
                tweet[key] = value
        print(tweet)
        #tweetsConnection().insert_one(tweet)
    print("Terminada migração")

#migration()

#tweetsConnection().insert_one(item1)
#item1 = {'_id': 1605951450831589380, 'corpott': '', 'likes': 122, 'retweets': 17, 'rtcomment': 1, 'comment': 3, 'urlimg': 'https://pbs.twimg.com/media/Fkl8VYzWYAwTpT2.jpg', 'mediatype': 'photo', 'dtcreated': '2022-12-22 15:40:37', 'dtatt': '2023-01-10 13:53:09'}
