from kafka import KafkaConsumer
import json
import time

# Configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'my_topic_name'
consumer_group_id = 'my_consumer_group'

def consume_messages():
    """Consume messages from the Kafka topic"""
    try:

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group_id,
            auto_offset_reset='earliest',  
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,  
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"Consumer started. Listening for messages on topic '{topic_name}'...")
        

        for message in consumer:
            partition = message.partition
            offset = message.offset
            key = message.key.decode('utf-8') if message.key else None
            value = message.value
            timestamp = message.timestamp
            
            timestamp_str = time.strftime('%Y-%m-%d %H:%M:%S', 
                                         time.localtime(timestamp/1000))
            

            print(f"Received message:")
            print(f"  Partition: {partition}")
            print(f"  Offset: {offset}")
            print(f"  Key: {key}")
            print(f"  Value: {value}")
            print(f"  Timestamp: {timestamp_str}")
            print("-" * 50)
            
            
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            print("Consumer closed")

if __name__ == "__main__":
    consume_messages()