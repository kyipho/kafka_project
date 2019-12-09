"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON
TOPICS_LIST = topic_check.list_topics()
TURNSTILE_TOPICS_LIST = [t for t in TOPICS_LIST if 'turnstile_' in t]

# initialise query variable. this will eventually be sent to the /ksql endpoint
KSQL_OVERALL = ""

'''
Steps to build query:
1) For each turnstile, create a stream from the corresponding turnstile topic
2) Combine all turnstile streams into a single 'turnstile_combined_stream'
3) Use 'turnstile_combined_stream' to create a 'turnstile' table
4) Use 'turnstile' table to create a 'turnstile_summary' table
'''
# template for creating streams from each turnstile topic (topics created by running simulation)
STEP_1_CREATE_STREAM = """
CREATE STREAM {topic} (
    station_id VARCHAR,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{topic}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

"""

# step 1 complete: loop and build the stream creation statements
for topic in TURNSTILE_TOPICS_LIST:
    KSQL_OVERALL += STEP_1_CREATE_STREAM.format(topic=topic)

# statement to combine all separate turnstile streams into a single stream with output topic 'turnstile'.
# start with first topic, then insert all remaining topics into stream.
STEP_2_CREATE_COMBINED_STREAM = """
CREATE STREAM turnstile_combined_stream
WITH (
    KAFKA_TOPIC='turnstile_combined_stream',
    PARTITIONS=1,
    VALUE_FORMAT='AVRO'
) AS 
    SELECT * FROM {first_topic};

""".format(first_topic=TURNSTILE_TOPICS_LIST[0])

STEP_2A_INSERT_STREAMS = """
INSERT INTO turnstile_combined_stream
SELECT * FROM {topic};

"""

# build combined stream
for topic in TURNSTILE_TOPICS_LIST[1:]:
    STEP_2_CREATE_COMBINED_STREAM += STEP_2A_INSERT_STREAMS.format(topic=topic)

# comment/uncomment to debug:
# KSQL_OVERALL = ''

# step 2 complete:
KSQL_OVERALL += STEP_2_CREATE_COMBINED_STREAM

# using the combined stream, create a table
STEP_3_CREATE_TABLE = """
CREATE TABLE turnstile (
    station_id VARCHAR,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='turnstile_combined_stream',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);
"""

# comment/uncomment to debug:
# KSQL_OVERALL = ''

# step 3 complete:
KSQL_OVERALL += STEP_3_CREATE_TABLE

# using created table, create a summary table
STEP_4_CREATE_SUMMARY_TABLE = """
CREATE TABLE turnstile_summary
WITH (
    KAFKA_TOPIC='turnstile_summary',
    VALUE_FORMAT='JSON'
) AS
SELECT
    station_id,
    station_name,
    COUNT(*) AS count
FROM turnstile
GROUP BY
    station_id,
    station_name;
"""

# comment/uncomment to debug:
# KSQL_OVERALL = ''

# step 4 complete:
KSQL_OVERALL += STEP_4_CREATE_SUMMARY_TABLE

# print(KSQL_OVERALL)
# burn it all if something goes wrong. must terminate all queries, and delete extra topics first.
KSQL_DROP_STREAMS = ""
for topic in TURNSTILE_TOPICS_LIST:
    KSQL_DROP_STREAMS += f'DROP STREAM IF EXISTS {topic}; '

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print('TURNSTILE_SUMMARY topic exists, delete it first')
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        # note that SELECT statements have to be POSTed to /query endpoint, not /ksql
        f"{KSQL_URL}/ksql",
#         f"{KSQL_URL}/query",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_OVERALL,
#                 "ksql": KSQL_DROP_STREAMS,
                "streamsProperties": {
                    "ksql.streams.auto.offset.reset": "earliest",
                    "ksql.streams.cache.max.bytes.buffering": '30000000'
                },
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
