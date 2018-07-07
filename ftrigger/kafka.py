import atexit
import collections
import logging
import os
import threading
import time
import datetime
import multiprocessing

try:
    import ujson as json
except:
    import json
import pyjq
from kafka import KafkaConsumer, KafkaProducer

from .trigger import Functions


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


#class OpenFaasKafkaConsumer(threading.Thread):
class OpenFaasKafkaConsumer(multiprocessing.Process):
   def __init__(self, thread_id, config, functions, topic_name, partition_no):
      #threading.Thread.__init__(self)
      multiprocessing.Process.__init__(self)
      self.stop_event = multiprocessing.Event()
      #self.setDaemon(True)
      self.thread_id = thread_id
      # instantiate functions
      self.functions = Functions(name='kafka')
      self.functions.refresh_interval=10
      self.topic_name = topic_name
      self.partition_no = partition_no
      # Reset the config 
      self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'group.id': 'group' + topic_name,
            'fetch.wait.max.ms': 20,
            #'debug': 'cgrp,topic,fetch,protocol',
            'auto.offset.reset': 'earliest',
            'auto.commit.interval.ms': 5000,
            'consumer.timeout.ms': 20
            
      }
      log.debug('Instantiating thread: ' + self.thread_id)
      log.info('Instantiating thread: ' + self.thread_id)
        
   def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key
        
   def stop(self):
        self.stop_event.set()  
        
   def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.config['bootstrap.servers'],
                                 auto_offset_reset=self.config['auto.offset.reset'],
                                 consumer_timeout_ms=self.config['consumer.timeout.ms'],
                                 auto_commit_interval_ms=self.config['auto.commit.interval.ms'],
                                 group_id=self.config['group.id'])
        
        # if we want to manually assign parition to a consume, enable this line
        #consumer.assign([TopicPartition(self.topic_name, self.partition_no)])
        
        consumer.subscribe([str(self.topic_name)])
        
        log.debug('Executing a consumer with ID: ' + self.thread_id)
        log.info('Executing a consumer with ID: ' + self.thread_id)
        
        callbacks = collections.defaultdict(list)
        functions = self.functions

        def close():
            log.info('Closing consumer in thread: ' +  self.thread_id)
            consumer.close()
        atexit.register(close)

        while True:
            add, update, remove = functions.refresh()
            if add or remove:
                  for f in add:
                     if f not in callbacks[self.topic_name]:
                        callbacks[self.topic_name].append(f)
                
                  for f in remove:
                     if f in callbacks[self.topic_name]:
                         callbacks[self.topic_name].remove(f)

            consumer.poll(timeout=1.0)
            #message = consumer.poll(timeout=1.0)
            
            for message in consumer:
                log.debug('Processing a message in thread: ' +  self.thread_id)

                if not message:
                    log.debug('Empty message received')
                    pass
                else:
                    topic, key, value = message.topic, \
                                        message.key, \
                                        message.value


                    log.debug('Processing topic: ' + str(topic) + ' : in thread: ' + self.thread_id)
                    try:
                        key = message.key.decode('utf-8')
                        log.debug('Processing Key: ' + str(key) + ' : in thread: ' + self.thread_id)
                    except:
                        log.debug('Key could not be decoded in thread: ' + self.thread_id )
                        pass
                    try:
                        value = json.loads(value)
                        log.debug('Processing value: ' + str(value) + ' : in thread: ' + self.thread_id)
                    except:
                        log.debug('Value could not be decoded in thread: ' + self.thread_id )
                        pass


                    for function in callbacks[topic]:
                        jq_filter = functions.arguments(function).get('filter')
                        try:
                            if jq_filter and not pyjq.first(jq_filter, value):
                                continue
                        except:
                            log.error(f'Could not filter message value with {jq_filter}')

                        data = self.function_data(function, topic, key, value)
                        log.debug('In thread:' + self.thread_id + ' : Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                        log.info('In thread:' + self.thread_id + ' : Function: ' + f'/function/{function["name"]}' + ' Data:' + data )

                        functions.gateway.post(functions._gateway_base + f'/function/{function["name"]}', data=data)

class KafkaTrigger(object):

    def __init__(self, label='ftrigger', name='kafka', refresh_interval=5,
                 kafka='kafka:9092'):
        self.functions = Functions(name='kafka')
        self.functions.refresh_interval=10
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'group.id': 'group' + topic_name,
            'fetch.wait.max.ms': 20,
            #'debug': 'cgrp,topic,fetch,protocol',
            'auto.offset.reset': 'earliest',
            'auto.commit.interval.ms': 5000,
            'consumer.timeout.ms': 20
            
         }
    
    def run(self):
         
         topic_list_with_consumers = []
         no_of_paritions = os.getenv('NUMBER_OF_CONSUMERS_PER_TOPIC', 5)
         no_of_paritions = int(no_of_paritions)                                
         log.debug('Number of Consumers:' + str(no_of_paritions))                                 

         callbacks = collections.defaultdict(list)
         functions = self.functions
         
                                           
         while True:
             add, update, remove = functions.refresh()
             existing_topics = set(callbacks.keys())
             new_candidate_topics = []
             consumer_threads = []
             
             for topic in existing_topics:
                 if topic not in topic_list_with_consumers:
                    new_candidate_topics.append(topic)
             
             # if new functions added
             if add:
                 for f in add:
                     if functions.arguments(f).get('topic') not in topic_list_with_consumers:
                         new_candidate_topics.append(functions.arguments(f).get('topic'))
                                           
             new_candidate_topics = set(new_candidate_topics)
             for topic_name in new_candidate_topics:
               for partition_no in range(no_of_paritions):    
                  con_thread = OpenFaasKafkaConsumer(topic_name + '-' + str(partition_no), self.config, self.functions, topic_name, partition_no)
                  consumer_threads.append(con_thread)
               
               topic_list_with_consumers.append(topic_name)
               print(topic_list_with_consumers)
             
       
             
             for t in consumer_threads:
                t.start()

def main():
    trigger = KafkaTrigger()
    trigger.run()
