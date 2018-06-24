import atexit
import collections
import logging
import os
import threading
import time

try:
    import ujson as json
except:
    import json
import pyjq
from confluent_kafka import Consumer, TopicPartition

from .trigger import Functions


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class OpenFassKafkaConsumer(threading.Thread):
   def __init__(self, thread_id, config, functions, topic_name, partition_no):
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      # instantiate functions
      self.functions = Functions(name='kafka')
      self.topic_name = topic_name
      self.partition_no = partition_no
      # Reset the config 
      self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'group.id': 'group' + topic_name,
            'default.topic.config': {
                'auto.offset.reset': 'largest',
                'auto.commit.interval.ms': 5000
            }
      }
      log.debug('Instantiating thread: ' + self.thread_id)
   def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key
        
   def run(self):
        consumer = Consumer(self.config)
        consumer.assign([TopicPartition(self.topic_name, self.partition_no)])
        
        log.debug('Executing a consumer with ID: ' + self.thread_id)
        
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
                     if functions.arguments(f).get('topic') not in callbacks[self.topic_name]:
                        callbacks[self.topic_name].append(f)
                
                  for f in remove:
                     if functions.arguments(f).get('topic') in callbacks[self.topic_name]:
                         callbacks[self.topic_name].remove(f)

            message = consumer.poll(timeout=functions.refresh_interval)
            if not message:
                log.debug('Empty message received')
            elif not message.error():
                topic, key, value = message.topic(), \
                                    message.key(), \
                                    message.value()
            
                log.debug('Processing a message in thread: ' +  self.thread_id)
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
                
                             
                for function in callbacks[topic]:
                    jq_filter = functions.arguments(function).get('filter')
                    try:
                        if jq_filter and not pyjq.first(jq_filter, value):
                            continue
                    except:
                        log.error(f'Could not filter message value with {jq_filter}')
                    
                    data = self.function_data(function, topic, key, value)
                    log.debug('In thread:' + self.thread_id + ' : Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                    
                    functions.gateway.post(functions._gateway_base + f'/function/{function["name"]}', data=data)

class KafkaTrigger(object):

    def __init__(self, label='ftrigger', name='kafka', refresh_interval=5,
                 kafka='kafka:9092'):
        self.functions = Functions(name='kafka')
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', kafka),
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP', self.functions._register_label),
            'default.topic.config': {
                'auto.offset.reset': 'largest',
                'auto.commit.interval.ms': 5000
            }
        }
    
    def run(self):
         topic_list_with_consumers = []
         no_of_paritions = 10

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
             
             for topic_name in new_candidate_topics:
               for partition_no in range(no_of_paritions):    
                  con_thread = OpenFassKafkaConsumer(topic_name + '-' + str(partition_no), self.config, self.functions, topic_name, partition_no)
                  consumer_threads.append(con_thread)
               
               topic_list_with_consumers.append(topic_name)
             
             print(topic_list_with_consumers)
             
             for t in consumer_threads:
                t.start()
                t.join()

def main():
    trigger = KafkaTrigger()
    trigger.run()
