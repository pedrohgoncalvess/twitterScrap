import json
import requests
from Databases.PostgreSQL import connect,duplicate_id
from psycopg2.errors import UniqueViolation

cursor = connect().cursor()

def scrapMessages():
    index_s = 0
    id_disc = '1056318073445040208'

    while index_s < 1000:

        headers = {
            "authorization":"NTMwNzc0OTI5NzYzOTkxNTcy.GNoVGp.eIK1MzeucDwFuvkIpGTsH6WtrPVEhWq9J6Dcvw"
        }


        url = f"https://discord.com/api/v9/channels/961719887670149181/messages?before={id_disc}"

        resp = requests.get(url,headers=headers)

        jsonn = json.loads(resp.text)
        index = 0
        for value in jsonn:
            if value['author']['username'] == 'sseramemes':
                try:
                    id_tt = value['content']
                    id_tt = id_tt.split("/")
                    id_tt = id_tt[5]
                    id_tt = id_tt.replace("🚀","")
                    if duplicate_id('scrapdiscordmessages',id_tt) == False:
                        query = f"INSERT INTO scrapDiscordMessages (idtweet) VALUES ({id_tt})"
                        cursor.execute(query)
                        print("Insert realizado com sucesso")
                    else:
                        print(f"O tweet {id_tt} já existe no DB.")
                except:
                    pass
            else:
                pass
            index += 1
            if index == 50:
                id_disc = value['id']

            else:
                pass


        index_s += 1


