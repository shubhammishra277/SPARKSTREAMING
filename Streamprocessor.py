from pyspark import  SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils 


Class streamprocessing(object):

         def __init__(self):
             
              conf=SparkConf().setMaster("local[2]").setAppName("Stream processor")
              sc=SparkContext(conf=conf)
              self.ssc=StreamingContext(sc,10)
              
              
         def dataprocessing(self,topic,bootstrapserverlist):
             
             ssc=self.ssc
             parallelism=5
             kstreams=[KafkaUtils.createDirectStream(ssc,topic=["TwitterStream"],kafkaParams=bootstrapserverlist) for i in parallelism]
             ksteam=ssc.union(*kstreams)
             tweetdata=kstream.flatmap(lambda x: [i for i in x.split(" ") if x.startswith("#")])
             tweetdata60=tweetdata.map(lambda x: (x,1)).reduceByKeyandWindow(lambda x,y:x+y,60,10).map(lambda x,y:y,x).sortBykey(False)
             tweetdata120=tweetdata.map(lambda x: (x,1)).reduceByKeyandWindow(lambda x,y:x+y,120,30).map(lambda x,y:y,x).sortBykey(False)
             word60.foreachRDD(lambda rdd :rdd.collect().foreach(lambda x,y:print("%s,%s"%(y,x)) ))
             word60.foreachRDD(lambda rdd :rdd.collect().foreach(lambda x,y:print("Hastag:%s, Frequency :%s"%(y,x)) ))
     
             ssc.start()
             ssc.awaitTermination()
             
             
             
             
             
if __name__=="__main__":
    
    bootstraplist=sys.agrv[1].split(",")
    topic=sys.argv[2]
    t1=streamprocessing(bootsraplist,topic)    
             
             
                    
             
             
              
              
