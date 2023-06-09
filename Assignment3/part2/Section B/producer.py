from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers="localhost:9092")


with open("/Users/hatsukoi/Desktop/Assignment3/resourses/17GB.log", "r") as file:
    lines = file.readlines()
    for line in lines:
        producer.send("logs", line.encode())  # convert string to bytes
        # time.sleep(10)

producer.close()
