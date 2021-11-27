from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime

def delete_topics(admin, topic):
    admin.delete_topics(topics=topic)


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    date = datetime.now().strftime("%m%d%M")

    admin_client = KafkaAdminClient(bootstrap_servers="35.193.37.189:9092",
                                    client_id='G9_Assignment_2_824')  # use your VM's external IP Here!
    topic_list = [NewTopic(name=f"records{date}", num_partitions=1, replication_factor=1)]
    #delete_topics(admin_client, ['records'])
    create_topics(admin_client, topic_list)

