'''
Created on 01-Aug-2018

@author: shubham


'''

import tweepy
import os
import time 
import optparse
from loggermodule import logger_test
import sys

reload(sys)
import kafkaproducermethod
from configgetter import configparserdata

sys.setdefaultencoding('utf8')





class MyStreamListener(tweepy.StreamListener):
    def __init__(self,api,bootstraplist,topic):
        self.api=api
        super(tweepy.StreamListener,self).__init__()
        self.k1=kafkaproducermethod(bootstraplist,topic)

    def on_status(self, status):
        #print "inside on_status method"
        print "Tweet",status.text
        self.k1.pushdata(status.text)
        return True
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False


class twitter_data_push():
    
    def __init__(self,client_name,searchstring,filepath):
        
        t1=configparserdata()
        platformname="twitter"
        self.filepath=filepath
        self.searchstring=searchstring.split(",")
        client_name,consumer_key,consumer_secret,access_token_key,access_token_secret,self.brokerlist,self.topic=t1.configparse(client_name, platformname)
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token_key, access_token_secret)
        try:
          self.api = tweepy.API(self.auth)
    
          logger_test.info("succesful authentication with twitter")
        except Exception,e:
            logger_test.exception("error occured while doing authentication with twitter with following error:%s"%str(e))
            sys.exit("authentication failed")
            
    def get_data(self):
        
        logger_test.debug("inside get_data function")
        myStream = tweepy.Stream(self.auth, listener=MyStreamListener(self.api,self.brokerlist,self.topic))
        logger_test.info("self.searchstring %s"%self.searchstring)
        myStream.filter(track=self.searchstring,async=True)
        
        
if __name__=="__main__":
    parser = optparse.OptionParser(description='Optional app description')
    parser.add_option('-u','--username', 
                    help='enter the username for which you have access token,consumer_token etc.')
    parser.add_option('-s','--search_string',
                    help='Query string which you want to filter for realtimedata',
                    default="Narendra Modi")
    try:
      options, args = parser.parse_args()
    except Exception,e:
        logger_test.exception("Sys arguements parsing failed with following errors:%s"%str(e))
    logger_test.info("arguemts parsed from command line are:%s "%options)
    
    
    t1=twitter_data_push(str.lower(options.username),options.search_string)
    t1.get_data()