"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL_KAFKA = "PLAINTEXT://localhost:9092"
BROKER_URL_SCHEMA_REGISTRY = "http://localhost:8081" 

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.admin_client = AdminClient({"bootstrap.servers": BROKER_URL_KAFKA})
        #
        #
        # DONE: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        # for other configuration: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        self.broker_properties = {
            'bootstrap.servers': BROKER_URL_KAFKA,
            'schema.registry.url': BROKER_URL_SCHEMA_REGISTRY,
            'group.id': 'client_producer_jh',
        }

        # DONE: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # DONE: Write code that creates the topic for this producer if it does not already exist on the Kafka Broker.
        
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            Producer.existing_topics.add(self.topic_name)

            futures = self.admin_client.create_topics([
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_partitions,
                )
            ])
            for _, future in futures.items():
                try:
                    future.result()
                    print(f'topic {self.topic_name} created successfully!')
                except Exception as e:
                    print(f'failed to create topic {self.topic_name}: {e}')
        else:
            logger.info(f"topic {self.topic_name} already exists on producer")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # DONE: Write cleanup code for the Producer here
        #
        if self.producer is not None:
            self.producer.flush()
            logger.info("producer closed successfully")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
