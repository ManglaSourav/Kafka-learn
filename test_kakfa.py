from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Kafka server connection
bootstrap_servers = 'localhost:29092'  # Change to your Kafka bootstrap server

# Create an Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers,
    client_id='my_client'
)

# Topic parameters
topic_name = 'my_topic_from_python'
partitions = 3
replication_factor = 1

# Create a new topic
new_topic = NewTopic(
    name=topic_name,
    num_partitions=partitions,
    replication_factor=replication_factor
)

# Create the topic
try:
    admin_client.create_topics([new_topic])
    print(f"Topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"Error creating topic: {e}")
finally:
    admin_client.close()
