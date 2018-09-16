import atexit
import collections
import logging
import os
import requests

try:
    import ujson as json
except:
    import json
import pyjq
from confluent_kafka import Consumer

from .trigger import Functions

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class KafkaTrigger(object):

    def __init__(self, label='ftrigger', name='kafka', refresh_interval=5,
                 kafka='kafka:9092'):
        self.functions = Functions(name='kafka')
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'debug': 'cgrp,topic,fetch,protocol',
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP', self.functions._register_label),
            'fetch.wait.max.ms': int(os.getenv('FETCH_MAX_WAIT_MS', 20)),
            'default.topic.config': {
                'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'latest'),
                'enable.auto.commit': bool(os.getenv('ENABLE_AUTO_COMMIT', 'True')),
                'auto.commit.interval.ms': int(os.getenv('AUTO_COMMIT_INTERVAL_MS', 5000))
            }
        }

    def run(self):
        consumer = Consumer(self.config)
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
                try:
                    key = message.key().decode('utf-8')
                except:
                    pass
                try:
                    value = json.loads(value)
                except:
                    pass
                for function in callbacks[topic]:
                    jq_filter = functions.arguments(function).get('filter') 
                    try:
                        if jq_filter and not pyjq.first(jq_filter, value):
                            continue
                    except:
                        log.error(f'Could not filter message value with {jq_filter}')
                    data = self.function_data(function, topic, key, value)
                   
                    function_directcall_url = functions.arguments(function).get('upstream_url')
                    
                    
                    if function_directcall_url:
                        headers = {"Content-Type" : "application/json"}
                        requests.post(str(function_directcall_url), data=data, headers=headers)
                        log.debug('Direct function call url:' + str(function_directcall_url) + ' function:' + f'/function/{function["name"]}' + ' Data:' + data )
                    else:
                        functions.gateway.post(functions._gateway_base + f'/function/{function["name"]}', data=data)
                        log.debug('Function: ' + f'/function/{function["name"]}' + ' Data:' + data )
                
                # if auto commit not enabled, manually commit the messages
                if not bool(os.getenv('ENABLE_AUTO_COMMIT', 'True')):
                   current_msg = message
                   consumer.commit(message=current_msg,async=False)

    def function_data(self, function, topic, key, value):
        data_opt = self.functions.arguments(function).get('data', 'key')

        if data_opt == 'key-value':
            return json.dumps({'key': key, 'value': value})
        else:
            return key


def main():
    trigger = KafkaTrigger()
    trigger.run()
