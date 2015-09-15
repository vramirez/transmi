import oauth2 as oauth
import urllib2 as urllib
import sys
import json
import os

# See assignment1.html instructions or README for how to get these credentials

api_key = "<whatever>"
api_secret = "<whatever>"
access_token_key = "<whatever>"
access_token_secret = "<whatever>"

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url, 
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchtransmi(opc):
  #url = "https://stream.twitter.com/1/statuses/sample.json"
  url = "https://api.twitter.com/1.1/search/tweets.json"
  #opc = sys.argv[1]}
  opc=str(opc)
  parameters={}
  if opc == "1":
  	parameters = {'count':'500','q': "transmilenio hijueputas"}
  elif opc== "2":
	parameters = {'count':'500','q': "transmilenio hijueputa"}
  elif opc == "3":
	parameters = {'count':'500','q': "transmilenio malparidos"}
  elif opc == "4":
	parameters = {'count':'500','q': "transmilenio malparido"}
  elif opc== "5":
	parameters = {'count':'500','q': "to:transmilenio hijueputa"}
  elif opc == "6":
	parameters = {'count':'500','q': "to:transmilenio hijueputas"}
  elif opc == "7":
	parameters = {'count':'500','q': "to:transmilenio malparido"}
  elif opc == "8":
	parameters = {'count':'500','q': "to:transmilenio malparidos"}
  elif opc == "9":
	parameters = {'count':'500','q': "to:transmilenio"}
  elif opc == "10":
	parameters = {'count':'500','q': "@transmilenio"}
  elif opc == "11":
	parameters = {'count':'500','q': "transmilenio mal servicio"}
  elif opc == "12":
	parameters = {'count':'500','q': "transmilenio pesimo"}
  sais=0
  #print parameters
  response = twitterreq(url, "GET", parameters)
  tuits=json.load(response)
  txt=open('transmiall.json','aw')
  txt.write('KEYWORDZZZZ '+parameters['q']+"\n")
  for tuit in tuits['statuses']:
      sais+=1
      
      if 'text' in tuit:
 	  #print (tuit['user'])['screen_name'],(tuit['user'])['name'],tuit['text'],tuit['created_at']
	  #tw=tuit['text']+"|"+tuit['created_at']
	  #tw=tw.encode('utf-8')
	  txt.write(str(tuit))
	  txt.write('\n')
      else:
  	  print "borradinho"
  txt.close()
  print sais,"en",parameters

if __name__ == '__main__':
	if os.path.exists('transmiall.json'):
		os.remove('transmiall.json')
	for i in range(1,13):
	  fetchtransmi(i)
