from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json
import time

# Configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'my_topic_name'
num_partitions = 3
replication_factor = 1  # Use higher value in production (typically 3)

def create_topic():
    """Create a new Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )
        
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            print(f"Topic '{topic_name}' already exists")
            return
        
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully")
        
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        if 'admin_client' in locals():
            admin_client.close()

def publish_messages(sent_data):
    """Publish custom data to the Kafka topic"""
    try:
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Publish messages
        for data in sent_data:
            # Key is optional - using id as the key in this example
            key = str(data["id"]).encode('utf-8')
            
            # Send message
            future = producer.send(
                topic=topic_name,
                key=key,
                value=data
            )
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            
            # Add small delay between messages
            # time.sleep(0.5)
            
    except Exception as e:
        print(f"Error publishing messages: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()
            producer.close()

if __name__ == "__main__":
    # First create the topic
    create_topic()
    
    # Then publish messages
    publish_messages()