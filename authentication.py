import tweepy
from dotenv import load_dotenv
import os

load_dotenv()

def environment_variables(variable:str) -> str:
    val = os.getenv(variable) #USER DATABASE
    return val


# Authenticate to Twitter

def authentication():
    autentication = {
    'consumer_key' : environment_variables('consumer_key'),
    'consumer_secret' : environment_variables('consumer_secret'),
    'access_token' : environment_variables('access_token'),
    'access_token_secret' : environment_variables('access_token_secret'),
    'bearer_token' : environment_variables('bearer_token'),
}
    return autentication


def bearer_token():
    headers = {
        f"Authorization": f"Bearer {environment_variables('bearer_token')}"
    }

    return headers






