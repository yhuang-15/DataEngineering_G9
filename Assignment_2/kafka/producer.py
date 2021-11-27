from kafka import KafkaProducer
from csv import DictReader, reader
import json
from time import sleep
from datetime import datetime

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
    date = datetime.now().strftime("%m%d%M")
    # change ip every time
    producer = KafkaProducer(bootstrap_servers='35.193.37.189:9092')
    file_path = f"D:/2021-2023_MDSE/1.1/Data Engineering/Assignments/data/Credit_card_transactions/test_{5}.csv"


    with open(file_path) as f:
        lines = reader(f)

        # produce the records in csv file line by line
        for id, line in enumerate(lines):
            msg = ''
            for column in line:
                # remove extra comma in some column's value
                column = column.replace(',','')
                msg += column + ','
            kafka_python_producer_sync(producer, msg[:-1], 'record')
        
       

    
