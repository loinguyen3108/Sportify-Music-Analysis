import random
import time

from confluent_kafka import Consumer, OFFSET_BEGINNING

from src.configs.logger import get_logger
from src.configs.kafka import BROKER_URL


class SpotifyConsumer:
    MIN_DELAY = 1
    MAX_DELAY = 5

    def __init__(self, topics, group_id, messages_handler, offset_earliest=False,
                 consume_timeout=1.0, num_messages=1, should_sleep=False):
        self.logger = get_logger(self.__class__.__name__)
        self.topics = topics
        self.messages_handler = messages_handler
        self.consume_timeout = consume_timeout
        self.num_messages = num_messages
        self.offset_earliest = offset_earliest
        self.should_sleep = should_sleep
        self.consumer = Consumer({
            'bootstrap.servers': BROKER_URL,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': 3000000
        })
        self.consumer.subscribe(self.topics)

    def on_assign(self, consumer, partitions):
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
            self.logger.info(
                f'Partition offset set to beginning for {self.topics}')

        self.logger.info(f'Partitions assigned for {self.topics}')
        consumer.assign(partitions)

    def consume_messages(self):
        while True:
            try:
                messages = self.consumer.consume(
                    num_messages=self.num_messages, timeout=self.consume_timeout)
                if messages is None:
                    self.logger.info('No message received by consumer')
                    continue
                else:
                    self.messages_handler(messages)
                    self.logger.info(f'Consumed {len(messages)} messages')
            except KeyboardInterrupt:
                self.logger.info('Aborted by user')
                break

            if self.should_sleep:
                self.sleep()
        self.close()

    def close(self):
        self.logger.info(f'closing {self.topics}')
        self.consumer.close()

    def sleep(self):
        random_delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        self.logger.info(f'Delaying for {random_delay} seconds')
        time.sleep(random_delay)
