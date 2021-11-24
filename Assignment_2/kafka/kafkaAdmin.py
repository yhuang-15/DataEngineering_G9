from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin, topic):
    admin.delete_topics(topics=topic)


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="35.188.56.254:9092",
                                    client_id='G9_Assignment_2')  # use your VM's external IP Here!
    topic_list = [NewTopic(name="records", num_partitions=1, replication_factor=1)]
    #delete_topics(admin_client, ['records'])
    create_topics(admin_client, topic_list)

