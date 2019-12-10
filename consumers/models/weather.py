"""Contains functionality related to Weather"""
import logging
import json
from confluent_kafka import avro

logger = logging.getLogger(__name__)
REST_PROXY_URL = "http://localhost:8082"
CONSUMER_GROUP = "weather_consumer_group"
TOPIC_NAME = "org.chicago.weather_events"

class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        # TODO: Process incoming weather messages. Set the temperature and status.
        
        value = message.value()
        self.temperature = value["temperature"]
        self.status = value["status"]