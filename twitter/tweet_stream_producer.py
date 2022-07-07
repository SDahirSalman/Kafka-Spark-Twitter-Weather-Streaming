import credentials as t 
from tweepy import OAuthHandler
from tweepy import Stream, API
from kafka import KafkaProducer
import json
from bson import json_util
from dateutil.parser import parse
import re
import pyowm
import datetime

api_key = '3d2e4262254bb8b94eaf837891e9bc38'    #your API Key here as string
owm = pyowm.OWM(api_key).weather_manager()

class KafkaConfig():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
    def get_producer(self):
        return self.producer

class TweetStreamListener(Stream):

    def get_temp_data(self, city):
        try:
            d=re.search('^[\w ]+,', city)
            city=d.group()
        except:
            city = city
        try:
            weather_api = owm.weather_at_place(city)  # give where you need to see the weather
            weather_data = weather_api.weather          # get out data in the mentioned location
            wdict = weather_data.temperature('celsius')
            avg_temp = (wdict['temp_max'] + wdict['temp_min'])/2
            return avg_temp
        except:
            return 'null'
        
    def on_data(self, data):
        ##### Kafka producer initialize
        kafka_producer = KafkaConfig().get_producer()
        ##### Add the topic created
        kafka_topic = 'test'
        ##### Filter necessary fields from the json data
        tweet = json.loads(data)
        tweet_text = ""
        #print(data)

        if all(x in tweet.keys() for x in ['lang', 'created_at']) and tweet['lang'] == 'en':
                if 'retweeted_status'in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        if ('extended_tweet' in tweet['retweeted_status']['quoted_status'].keys()):
                            tweet_text = tweet['retweeted_status']['quoted_status']['extended_tweet']['full_text']
                    elif 'extended_tweet' in tweet['retweeted_status'].keys():
                        tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
                elif tweet['truncated'] == 'true':
                    tweet_text = tweet['extended_tweet']['full_text']

                else:
                    tweet_text = tweet['text']
        if(tweet_text):
            data = {
                'created_at': tweet['created_at'],
                 'message': tweet_text.replace(',',''),
                 'City': tweet.get('user', {}).get('location', {}),
                'Temp': self.get_temp_data(tweet.get('user', {}).get('location', {}))
                 }

        

            print(data)
            kafka_producer.send(kafka_topic, value = json.dumps(data, default=json_util.default).encode('utf-8'))
    
        def on_error(self, status):
            if (status == 420): 
                return False # 420 error occurs when rate limit exceeds
            print (status)
        

if __name__ == "__main__":
    
    listener = TweetStreamListener(t.consumer_key, t.consumer_secret, t.access_token,t.access_token_secret)
    #API Authentication
    auth = OAuthHandler(t.consumer_key, t.consumer_secret)
    auth.set_access_token(t.access_token, t.access_token_secret)
    api = API(auth)

    #stream = Stream(api.auth, listener)
    ###### Add the tracks to filter the tweets
    listener.filter(track=["Covid19", "coronavirus", "covid"])
