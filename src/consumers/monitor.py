from concurrent.futures import ThreadPoolExecutor
from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from src.crawler.base import BaseCrawler


class MonitorConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def monitor_deserializer(self):
        return self.get_deserialized(schema_name='monitor_value')

    def messages_handler(self, messages):
        object_kwargs = []
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            message_value = self.monitor_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            object_name = message_value['object_name']
            object_value = message_value['object_value']
            object_kwargs.append((object_name, object_value))

        with ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(
                self.spotify_service.monitor_crawler, *zip(*object_kwargs))
