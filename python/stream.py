import json
#import os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


# See assignment1.html instructions or README for how to get these credentials

api_key = "dhyp6pcBYdkW8KUGS3hOqrz26"
api_secret = "2E8YhCdTA6RGYRUI6oLJCLDzmahheOwlaI40AgpLK8EbGM7Yrf"
access_token_key = "17558870-fxmy2cgC4G7huMXbwIHcP9he1sJP2PcISkqHQaIZO"
access_token_secret = "1u2C6GW6n09kSSxcijd4nbC0nF6ikI9CaZLjoot2dWkD0"
filename = "transmi.json"
fp=open(filename,'aw')

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        #print data
	fp.write(data)
        return True

    def on_error(self, status):
        print status

class OderOutListener(StreamListener):

	def on_status(self, status):
		fp.write(status)
		return True 
	

if __name__ == '__main__':
	#if os.path.exists(filename):
	#	os.remove(filename)
	l = StdOutListener()
	o = OderOutListener()
    	auth = OAuthHandler(api_key, api_secret)
	auth.set_access_token(access_token_key, access_token_secret)
    	stream = Stream(auth, l)

    	#This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    	stream.filter(track=['transmilenio'])
