from __future__ import print_function
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')  
  
i = 0
while True:
    ts = int(time.time() * 1000)
    producer.send(topic="myfirsttopic", value=bytes(str(i).encode('utf-8')), timestamp_ms=ts)
    producer.flush()
    print(i)
    i += 1
    time.sleep(1)