'''
Created on 01-Aug-2018

@author: shubham
'''

from confluent_kafka import Producer
from loggermodule import logger_test
import sys


sys.setdefaultencoding('utf8')


class kafkaproducermethod(object):
    
    def __init__(self,broker,topic):
        
        self.broker=broker
        self.topic=topic
        
    
    def pushdata(self,i):
        conf = {'bootstrap.servers': self.broker}

    # Create Producer instance
        p = Producer(**conf)
        try:
                        
           p.produce(self.topic, i.rstrip(), callback=self.delivery_callback)

        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)

 
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
        p.flush()
    
    def delivery_callback(self,err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
            
            
            
            