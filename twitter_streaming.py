# To run this code, first edit config.py with your configuration, then:
#
# mkdir data
# python twitter_streaming.py -q weather -n 100 -d data
#
# This will produce a list of 100 tweets for the query "weather" in the file data/stream_weather.json

import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import argparse
import string
import config
import json

def get_parser():
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(description="Twitter Downloader")
    parser.add_argument("-q",
                        "--query",
                        dest="query",
                        help="Query/Filter",
                        default='-')
    parser.add_argument("-n",
                        "--number",
                        dest="max_tweets",
                        help="Max number of tweets")
    parser.add_argument("-d",
                        "--data-dir",
                        dest="data_dir",
                        help="Output/Data Directory")
    return parser


class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, data_dir, query, max_tweets):
        super(MyListener, self).__init__()
        self.num_tweets = 0
        self.max_tweets = int(max_tweets)
        query_fname = format_filename(query)
        self.outfile = "%s/stream_%s.json" % (data_dir, query_fname)

    def on_data(self, data):
        self.num_tweets += 1
        if self.num_tweets <= self.max_tweets:
            try:
                with open(self.outfile, 'a') as f:
                    f.write(data)
                    print(data)
                    return True
            except BaseException as e:
                print("Error on_data: %s" % str(e))
                time.sleep(5)
            return True
        else:
            return False

    def on_error(self, status):
        print(status)
        return True


def format_filename(fname):
    """Convert file name into a safe string.
    Arguments:
        fname -- the file name to convert
    Return:
        String -- converted file name
    """
    return ''.join(convert_valid(one_char) for one_char in fname)


def convert_valid(one_char):
    """Convert a character into '_' if invalid.
    Arguments:
        one_char -- the char to convert
    Return:
        Character -- converted char
    """
    # valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    valid_chars = '-_.' + string.ascii_letters + string.digits
    if one_char in valid_chars:
        return one_char
    else:
        return '_'


def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)

    twitter_stream = Stream(auth, MyListener(args.data_dir, args.query, args.max_tweets))
    twitter_stream.filter(track=[args.query])
