"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)

class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    # DONE: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # DONE: Complete the below by deciding on a topic name, number of partitions, and number of replicas
        super().__init__(
            "org.chicago.cta.station.turnstile_events",
            key_schema=Turnstile.key_schema,
            # TODO:
            value_schema=Turnstile.value_schema,
            # TODO:
            num_partitions=1,
            # TODO:
            num_replicas=1,
        )
        self.station_name = station_name
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)
        self.line = station.color.name
        self.station_id = station.station_id

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        
        # copy function logic from time_millis() in producer.py to get timestamp as a key
        # convert timestamp (in datetime.datetime format) to a key
        self.timestamp = int(round(timestamp.timestamp() * 1000))
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
#         logger.info("turnstile kafka integration incomplete - skipping")
        #
        # DONE: Complete this function by emitting a message to the turnstile topic for the number of entries that were calculated
        #
        # message value will also be used in logger
        message_value = {
            "station_id": str(self.station_id),
            "station_name": str(self.station_name),
            "line": str(self.line)
        }

        for i in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.timestamp},
                    value=message_value,
                    key_schema=Turnstile.key_schema,
                    value_schema=Turnstile.value_schema
                )
                logger.info(f"\n\nSUCCESSFULLY PRODUCED TURNSTILE MESSAGE\n{message_value}\n")
            except:
                logger.warn(f"\n\nFAILED TO PRODUCE TURNSTILE MESSAGE\n{message_value}\n")