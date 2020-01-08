"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# DONE: Define a Faust Stream that ingests data from the Kafka Connect stations topic and places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# DONE: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("connect-stations", value_type=Station)
# DONE: Define the output Kafka Topic
out_topic = app.topic(
    # must use 'org.chicago.cta.stations.table.v1' as topic name, based on server.py and lines.py
    "org.chicago.cta.stations.table.v1",
    key_type=int,
    value_type=TransformedStation,
    partitions=1
)
# DONE: Define a Faust Table
table = app.Table(
    "stations.table",
    default=str,
    partitions=1,
    changelog_topic=out_topic,
)


# DONE: Using Faust, transform input `Station` records into `TransformedStation` records. Note that "line" is the color of the station. So if the `Station` record has the field `red` set to true, then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        else:
            line = 'green'

        station_transformed = TransformedStation(
            station_id=str(station.station_id),
            station_name=station.station_name,
            order=station.order,
            line=line
        )
        
        await out_topic.send(
            key=str(station_transformed.station_id),
            value=station_transformed
        )
#


if __name__ == "__main__":
    app.main()

# to see results, run `python connector.py` first to start the connector, then `faust -A faust_stream worker -l info` or `python faust_stream.py worker` to start the faust worker, then `kafka-console-consumer --bootstrap-server localhost:9092 --topic org.chicago.cta.stations.table.v1 --from-beginning` to see results.