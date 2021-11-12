from kafka import KafkaProducer


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='35.188.133.126:9092')  # use your VM's external IP Here!
    file_path = "D:\\2021-2023_MDSE\\1.1\Data Engineering\DE2021\lab7\data\wordcount.txt"
    with open(file_path) as f:
        lines = f.readlines()

    for id, line in enumerate(lines):
        if id == 0:
            kafka_python_producer_sync(producer, line[3:], 'word')
        else:
            kafka_python_producer_sync(producer, line, 'word')

    f.close()
