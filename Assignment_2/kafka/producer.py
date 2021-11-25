from kafka import KafkaProducer
from csv import DictReader, reader
import json


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg + '\n')
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.122.35.201:9092')
    file_path = "D:/2021-2023_MDSE/1.1/Data Engineering/Assignments/data/Credit_card_transactions/test_2.csv"
    
    with open(file_path) as f:
        lines = reader(f)
        for id, line in enumerate(lines):
            msg = ''
            for column in line:
                column = column.replace(',','')
                msg += column + ','
            kafka_python_producer_sync(producer, msg[:-1], 'records')

    
