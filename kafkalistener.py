#!/usr/bin/env python
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from pykafka import KafkaClient
consumer_key = 'Qcgiz9RGTzoDGIE9xXp7I8g50'
consumer_secret = 'O9GAbvEB2sTpsWHQdqgckNHaH7kkYySZJFVUtQ0ivJZ3uJ3fPG'
access_token = '800349403894595584-CPaavTex9Mx15uw2KyZxlP6l3Y6HPOL'
access_secret = '6AwzMonNzowdo0Ri2NMNfqywIeCkSTJ02dg4oJAfbYzVu'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

class StdOutListener(StreamListener):

    data = []
    errorStats = []

    def on_status(self, status):
        client = KafkaClient(hosts='127.0.0.1:9092')

        topic = client.topics[b'tweets']
        with topic.get_producer(delivery_reports=False) as producer:

            # print status.text
            producer.produce(status.text.encode())
            print(status.text)

    def on_error(self, status):
        print(status)
        return True

l = StdOutListener()
stream = Stream(auth,l)
stream.filter(locations=[106.72,-6.23,106.93,-6.129])


