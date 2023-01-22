import tweepy
from dotenv import load_dotenv
import os
from tweepy.auth import OAuthHandler

load_dotenv()

def environment_variables(variable:str) -> str:
    val = os.getenv(variable) #USER DATABASE
    return val


# Authenticate to Twitter

def client_auth():
    client = tweepy.Client(
        consumer_key=environment_variables('consumer_key'),
        consumer_secret=environment_variables('consumer_secret'),
        access_token=environment_variables('access_token'),
        access_token_secret=environment_variables('access_token_secret'),
        bearer_token=environment_variables('bearer_token')
    )
    return client


def autenticacao():
    autentication = {
    'consumer_key' : environment_variables('consumer_key'),
    'consumer_secret' : environment_variables('consumer_secret'),
    'access_token' : environment_variables('access_token'),
    'access_token_secret' : environment_variables('access_token_secret'),
    'bearer_token' : environment_variables('bearer_token'),
}
    return autentication


def authandler():
    auth = OAuthHandler(environment_variables('consumer_key'),environment_variables('consumer_secret'),environment_variables('access_token'),environment_variables('access_token_secret'))
    api = tweepy.API(auth)
    return api


def bearer_token():
    headers = {
        f"Authorization": f"Bearer {environment_variables('bearer_token')}"
    }

    return headers






