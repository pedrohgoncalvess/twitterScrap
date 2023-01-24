import datetime
from datetime import datetime
import time
import tweepy
from authentication import environment_variables
from Databases.PostgreSQL import connect


class Streaming(tweepy.StreamingClient):


    def on_connect(self):
        print("Conexão realizada com sucesso")

    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:
            id_tt = tweet.id
            dtcreated = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
            self.addTweet(id_tt,dtcreated)

            time.sleep(0.2)

    def on_exception(self, exception):
        print("Tratamento de exceção", exception)
        exception = str(exception)
        self.on_connect()

    def on_connection_error(self):
        print("Tratamento de erro de conexão.")
        self.on_connect()

    def on_request_error(self, status_code):
        print("O erro de requisição.")
        self.on_connect()

    def log(self, log: str,level: str,status: str,method='streamingLogs'):
        pass


    def addTweet(self,idtweet,dtcreated):
        cursor = connect().cursor()
        query = f"insert into tweetsstandby (idtweet, dtcreated) " \
                f"select {idtweet},'{dtcreated}' " \
                f"where not exists (select 1 from tweetsstandby where idtweet = {idtweet})"
        cursor.execute(query)
        print(f"Insert do tweet {idtweet} finalizado")
        cursor.close()



stream = Streaming(bearer_token=environment_variables('bearer_token'))

stream.add_rules(tweepy.StreamRule("from:sseramemes"))
stream.add_rules(tweepy.StreamRule("from:TestSibiServer"))
stream.filter()





