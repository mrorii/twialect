#!/usr/bin/env python

import argparse
import datetime
import json
import sys
import time

from tweepy.api import API
from tweepy.auth import OAuthHandler
from tweepy.streaming import Stream, StreamListener

def get_oauth(consumer_key=None, consumer_secret=None,
              access_key=None, access_secret=None):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    return auth

class Listener(StreamListener):
    def on_status(self, status):
        status.created_at += datetime.timedelta(hours=9)
        if not status.lang == 'ja':
            return
        if not status.geo:
            return
        if 'urls' in status.entities and len(status.entities['urls']) > 0:
            return

        data = {
            'uid': status.user.id,
            'time': str(status.created_at),
            'following_count': status.user.friends_count,
            'followers_count': status.user.followers_count,
            'coords': status.geo['coordinates'],
            'mentions': status.entities['user_mentions'],
            'hashtags': status.entities['hashtags'],
            'text': status.text,
            'source': status.source,
        }

        sys.stdout.write(json.dumps(data))
        sys.stdout.write('\n')
        sys.stdout.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collect geo-tagged Japanese tweets')
    parser.add_argument('--consumer_key', required=True)
    parser.add_argument('--consumer_secret', required=True)
    parser.add_argument('--access_key', required=True)
    parser.add_argument('--access_secret', required=True)
    args = parser.parse_args()


    while True:
        # Twitter frequently disconnects the streaming api,
        # so catch such events and try to reconnect after some time
        # Refer to https://groups.google.com/forum/#!msg/tweepy/o0lpL3WRoyg/2QvDkJr0WvMJ
        try:
            auth = get_oauth(consumer_key=args.consumer_key,
                             consumer_secret=args.consumer_secret,
                             access_key=args.access_key,
                             access_secret=args.access_secret)
            stream = Stream(auth, Listener(), secure=True)
            stream.filter(languages=['ja'],
                          locations=[127.64249, 26.060477, 145.347467, 45.344336])
        except:
            time.sleep(30)
