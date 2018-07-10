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
from kafka import KafkaConsumer

from .trigger import Functions


logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

### This ftrigger version should be used when ordering of the messages is not important ###
### Messages can be handled out of order by different processes ###
#class OpenFaasKafkaConsumer(threading.Thread):
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
            'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'latest'),
            'auto.commit.interval.ms': os.getenv('AUTO_COMMIT_INTERVAL_MS', 5000),
            'fetch.wait.max.ms': os.getenv('FETCH_MAX_WAIT_MS', 10)
       }
      # fetch.wait.max.ms: maximum amount of time in milliseconds the server will block before answering the fetch request
      # if there isn’t sufficient data to immediately satisfy the requirement given by fetch_min_bytes (default:1 byte)
      # auto.offset.reset: A policy for resetting offsets on OffsetOutOfRange errors: ‘earliest’ will move to the oldest available message,
      # ‘latest’ will move to the most recent.
        
      log.debug('Instantiating thread: ' + self.thread_id)
      log.info('Instantiating thread: ' + self.thread_id)
        
   def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key
        
   def run(self):
        consumer = KafkaConsumer(str(self.topic_name), bootstrap_servers=self.config['bootstrap.servers'],
                                 auto_offset_reset=self.config['auto.offset.reset'],
                                 fetch_max_wait_ms=int(self.config['fetch.wait.max.ms']), # must be set to a low value
                                 group_id=self.config['group.id'])
        
        log.debug('bootstrap_servers: ' + self.config['bootstrap.servers'] + ' auto_offset_reset: ' + self.config['auto.offset.reset'])
        log.debug('fetch_max_wait_ms: ' + str(self.config['fetch.wait.max.ms']) + ' group_id: ' + self.config['group.id'])
        # if we want to manually assign parition to a consume, enable this line
        #consumer.assign([TopicPartition(self.topic_name, self.partition_no)])
        #consumer.subscribe([str(self.topic_name)])
        
        log.debug('Executing a consumer with ID: ' + self.thread_id)
        log.info('Executing a consumer with ID: ' + self.thread_id)
        
        callbacks = collections.defaultdict(list)
        functions = self.functions

        def close():
            log.info('Closing consumer in thread: ' +  self.thread_id)
            consumer.close()
        atexit.register(close)
        
        poll_time_out = int(os.getenv('POLL_TIME_OUT', 1000)) # milliseconds spent waiting in poll if data is not available in the buffer
        poll_max_records = int(os.getenv('MAX_POLL_RECORDS', 10000)) # maximum number of records returned in a single call to poll()
        
        start_time = datetime.datetime.now()  
        message_count = 0
        message_list = []
        
        while True:
            add, update, remove = functions.refresh()
            if add or remove:
                  for f in add:
                     if f not in callbacks[self.topic_name]:
                        callbacks[self.topic_name].append(f)
                
                  for f in remove:
                     if f in callbacks[self.topic_name]:
                         callbacks[self.topic_name].remove(f)

            
            consumer.poll(timeout_ms=poll_time_out, max_records=poll_max_records)
            
            for message in consumer:
                log.debug('Processing a message in thread: ' +  self.thread_id)

                if not message:
                    log.debug('Empty message received')
                    pass
                else:
                    message_count =  message_count + 1
                    message_list.append(message)

                    end_time = datetime.datetime.now()
                    elapsed_time = end_time - start_time
                    elapsed_time = elapsed_time.total_seconds()*1000  # Convert into miliseconds
                    # ignore time for the time being
                    #if (message_count % int(os.getenv('MAX_RECORDS_MSG_LIST', 1000)) == 0) or (elapsed_time >= int(os.getenv('MAX_WAIT_MSG_LIST', 5000))):
             
            log.debug('Message list size: ' + str(len(message_list)))
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
            'group.id': 'ConsumerGroup',
            'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'latest'),
            'auto.commit.interval.ms': os.getenv('AUTO_COMMIT_INTERVAL_MS', 5000),
            'fetch.wait.max.ms': os.getenv('FETCH_MAX_WAIT_MS', 10)
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
