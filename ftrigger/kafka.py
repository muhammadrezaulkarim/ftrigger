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
from confluent_kafka import Consumer, TopicPartition

from .trigger import Functions


logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

### This ftrigger version should be used when ordering of the messages is not important ###
### Messages can be handled out of order by different processes ###
class OpenFaasKafkaConsumer(multiprocessing.Process):
   def __init__(self, thread_id, config, functions, topic_name, partition_no):
      #threading.Thread.__init__(self)
      multiprocessing.Process.__init__(self)
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
            'fetch.wait.max.ms': int(os.getenv('FETCH_MAX_WAIT_MS', 20)),
            #'debug': 'cgrp,topic,fetch,protocol',
            'default.topic.config': {
                'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'latest'),
                'auto.commit.interval.ms': int(os.getenv('AUTO_COMMIT_INTERVAL_MS', 5000))
            }
      }
      log.debug('Instantiating thread: ' + self.thread_id)
      log.info('Instantiating thread: ' + self.thread_id)
        
   def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key
        
   def run(self):
        consumer = Consumer(self.config)
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
        
        message_count = 0
        message_list = []
        
        start_time = datetime.datetime.now()
        
        while True:
            add, update, remove = functions.refresh()
            if add or remove:
                  for f in add:
                     if f not in callbacks[self.topic_name]:
                        callbacks[self.topic_name].append(f)
                
                  for f in remove:
                     if f in callbacks[self.topic_name]:
                         callbacks[self.topic_name].remove(f)

            message = consumer.poll(timeout=1.0)
            #log.debug('Processing a message in thread: ' +  self.thread_id)
            
            if not message:
                log.debug('Empty message received')
                pass
            elif not message.error():
                message_count =  message_count + 1
                message_list.append(message)
                
                end_time = datetime.datetime.now()
                elapsed_time = end_time - start_time
                elapsed_time = elapsed_time.total_seconds()*1000  # Convert into miliseconds
               
                if (message_count % 1000 == 0) || (elapsed_time >= int(os.getenv('MAX_WAIT_MSG_LIST', 5000))):
                    if len(message_list) > 0:
                        msg_processor = OpenFaasMessageProcessor(self.thread_id, functions, message_list, callbacks)
                        msg_processor.start()
                        message_list = []
                        start_time =  end_time  # reset end time
                        
class OpenFaasMessageProcessor(multiprocessing.Process):
   def __init__(self, thread_id, functions, message_list, callbacks):
      multiprocessing.Process.__init__(self)
      self.thread_id = thread_id
      self.functions = functions
      self.message_list = message_list
      self.callbacks =  callbacks
      log.debug('Processing a new message list in thread: ' + self.thread_id)
        
   def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key
        
   def run(self):
        for message in self.message_list:
                topic, key, value = message.topic(), \
                                    message.key(), \
                                    message.value()
            
                
                log.debug('Processing topic: ' + str(topic) + ' : in thread: ' + self.thread_id)
                try:
                    key = message.key().decode('utf-8')
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
                
                             
                for function in self.callbacks[topic]:
                    jq_filter = self.functions.arguments(function).get('filter')
                    try:
                        if jq_filter and not pyjq.first(jq_filter, value):
                            continue
                    except:
                        log.error(f'Could not filter message value with {jq_filter}')
                    
                    data = self.function_data(function, topic, key, value)
                    log.debug('In thread:' + self.thread_id + ' : Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                    log.info('In thread:' + self.thread_id + ' : Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                    
                    self.functions.gateway.post(self.functions._gateway_base + f'/function/{function["name"]}', data=data)
       
                                           
class KafkaTrigger(object):

    def __init__(self, label='ftrigger', name='kafka', refresh_interval=5,
                 kafka='kafka:9092'):
        self.functions = Functions(name='kafka')
        self.functions.refresh_interval=10
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'group.id': 'group',
            'fetch.wait.max.ms': int(os.getenv('FETCH_MAX_WAIT_MS', 20)),
            #'debug': 'cgrp,topic,fetch,protocol',
            'default.topic.config': {
                'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'latest'),
                'auto.commit.interval.ms': int(os.getenv('AUTO_COMMIT_INTERVAL_MS', 5000))
            }
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
