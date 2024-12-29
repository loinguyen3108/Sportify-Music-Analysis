from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from src.configs.logger import get_logger
from src.configs.kafka import BROKER_URL, SCHEMA_REGISTRY


class SpotifyProducer:

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.producer = AvroProducer(
            config={
                'bootstrap.servers': BROKER_URL,
                'schema.registry.url': SCHEMA_REGISTRY
            }
        )

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(
                f'Delivery failed for User record {msg.key()}: {err}')
            return
        self.logger.info(
            f'User record {msg.key()} successfully produced to {msg.topic()} '
            f'[{msg.partition()}] at offset {msg.offset()}'
        )

    @staticmethod
    def topic_exists(client: AdminClient, topic_name: str) -> bool:
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self, topic_name: str, num_partitions: int = 1, num_replicas: int = 1):
        client = AdminClient({'bootstrap.servers': BROKER_URL})
        if not self.topic_exists(client, topic_name):
            client.create_topics([
                NewTopic(
                    topic=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=num_replicas,
                    config={
                        'cleanup.policy': 'delete',
                        'deletion.retention.ms': '86400000',
                        'retention.ms': '86400000'
                    }
                ),
            ])
            self.logger.info(f'{topic_name} has been created')
        else:
            self.logger.info(f'{topic_name} already exists')

    def close(self):
        self.producer.flush()

    def produce(self, topic: str, key: dict, value: dict, key_schema, value_schema):
        self.producer.poll(0.0)
        self.producer.produce(topic=topic, key=key, value=value,
                              key_schema=key_schema, value_schema=value_schema,
                              on_delivery=self.delivery_report)
