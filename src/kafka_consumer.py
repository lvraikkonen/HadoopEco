from __future__ import print_function
from kafka import KafkaConsumer

KAFKA_HOST = 'localhost'
KAFKA_PORT = 9092
KAFKA_TOPIC = 'myfirsttopic'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='localhost:9092'
)

try:
    for message in consumer:
        print(message)
except KeyboardInterrupt:
    print("Catch keyboard interrupt")