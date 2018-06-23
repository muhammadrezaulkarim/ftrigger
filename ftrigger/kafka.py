import atexit
import collections
import logging
import os
import threading

try:
    import ujson as json
except:
    import json
import pyjq
from confluent_kafka import Consumer

from .trigger import Functions


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class OpenFassKafkaConsumer(threading.Thread):
   def __init__(self, thread_id, config, functions, topic_name, partition_no):
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.functions = functions
      self.topic_name = topic_name
      self.partition_no = partition_no
      self.config = config
      self.config['group.id'] = 'group' + topic_name
 
   def run(self):
        consumer = Consumer(self.config)
        consumer.assign([TopicPartition(self.topic_name, self.partition_no)])
        
        callbacks = collections.defaultdict(list)
        functions = self.functions

        def close():
            log.info('Closing consumer')
            consumer.close()
        atexit.register(close)

        while True:
            add, update, remove = functions.refresh()
            if add or update or remove:
                existing_topics = set(callbacks.keys())

                for f in add:
                    callbacks[functions.arguments(f).get('topic')].append(f)
                for f in update:
                    pass
                for f in remove:
                    callbacks[functions.arguments(f).get('topic')].remove(f)

                interested_topics = set(callbacks.keys())

                if existing_topics.symmetric_difference(interested_topics):
                    log.debug(f'Subscribing to {interested_topics}')
                    consumer.subscribe(list(interested_topics))

            message = consumer.poll(timeout=functions.refresh_interval)
            if not message:
                log.debug('Empty message received')
            elif not message.error():
                topic, key, value = message.topic(), \
                                    message.key(), \
                                    message.value()
            
                log.debug('Processing a message:')
                log.debug('Topic:' + str(topic))
                try:
                    key = message.key().decode('utf-8')
                    log.debug('Key:' + str(key))
                except:
                    log.debug('Key could not be decoded.')
                    pass
                try:
                    value = json.loads(value)
                    log.debug('value:' + str(value))
                except:
                    log.debug('value could not be decoded.')
                    pass
                
                             
                for function in callbacks[topic]:
                    jq_filter = functions.arguments(function).get('filter')
                    try:
                        if jq_filter and not pyjq.first(jq_filter, value):
                            continue
                    except:
                        log.error(f'Could not filter message value with {jq_filter}')
                    
                    data = self.function_data(function, topic, key, value)
                    log.debug('Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                    
                    functions.gateway.post(functions._gateway_base + f'/function/{function["name"]}', data=data)

    def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key

class KafkaTrigger(object):

    def __init__(self, label='ftrigger', name='kafka', refresh_interval=5,
                 kafka='kafka:9092'):
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', kafka),
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP', self.functions._register_label),
            'default.topic.config': {
                'auto.offset.reset': 'largest',
                'auto.commit.interval.ms': 5000
            }
        }
       
        self.functions = Functions(name='kafka')
    
    def run(self):
         topic_list = ['mrkcse.test']
         no_of_paritions = 5
         consumer_threads = []
         
         for topic_name in topic_list:
           for partition_no in range(no_of_paritions):                                
             con_thread = OpenFassKafkaConsumer(topic_name + '-' + str(partition_no), self.config, self.functions, topic_name, partition_no)
             consumer_threads.append(con_thread)
         
         for t in consumer_threads:
            t.start()
         
         while True:
            time.sleep(10)

def main():
    trigger = KafkaTrigger()
    trigger.run()
