from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField

from src.crawler.base import BaseCrawler


class TrackPlaysCountConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def track_deserializer(self):
        return self.get_deserialized(schema_name='track_value')

    def messages_handler(self, messages):
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            message_value = self.track_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            track_id = message_value['track_id']
            self.spotify_service.crawl_track_plays_count_by_id(track_id)
