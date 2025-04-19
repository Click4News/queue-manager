import boto3
import json
import logging
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_or_create_queue(queue_name):
    """
    Get the URL for an SQS queue, creating it if it doesn't exist
    
    Parameters:
    queue_name (str): The name of the SQS queue
    
    Returns:
    str: The queue URL
    """
    # Initialize SQS client with specific region
    region_name = 'us-east-2'  # Replace with your actual region like 'us-east-1'
    sqs = boto3.client('sqs', region_name=region_name)
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        # logger.info(f"Found existing queue: {queue_name}")
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            logger.info(f"Queue {queue_name} does not exist. Creating it...")
            response = sqs.create_queue(QueueName=queue_name)
            logger.info(f"Successfully created queue: {queue_name}")
            return response['QueueUrl']
        else:
            logger.error(f"Error getting queue URL: {e}")
            raise

def push_message_to_sqs(queue_name, message_body, attributes=None, should_i_log=False):
    """
    Push a message to an SQS queue
    
    Parameters:
    queue_name (str): The name of the SQS queue
    message_body (dict/str): The message body to send
    attributes (dict, optional): Message attributes
    
    Returns:
    dict: The response from SQS containing MessageId if successful
    """
    # Initialize SQS client with specific region
    region_name = 'us-east-2'  # Replace with your actual region like 'us-east-1'
    sqs = boto3.client('sqs', region_name=region_name)
    
    try:
        # Get queue URL (create if it doesn't exist)
        queue_url = get_or_create_queue(queue_name)
        # logger.info(f"Using queue URL: {queue_url}")
        
        # Convert message body to string if it's a dict
        if isinstance(message_body, dict):
            message_body = json.dumps(message_body)
        
        # Prepare message attributes if provided
        message_attributes = {}
        if attributes:
            for key, value in attributes.items():
                message_attributes[key] = {
                    'DataType': 'String',
                    'StringValue': str(value)
                }
        
        # Send message to SQS
        if message_attributes:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes
            )
        else:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body
            )

        if should_i_log:
            logger.info(f"-----------Message sent! Message ID: {response['MessageId']} body={message_body}------------")
        return response
    
    except ClientError as e:
        logger.error(f"Error sending message to SQS: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    QUEUE_NAME = 'test-queue'
    
    # Example 1: Sending a simple string message
    push_message_to_sqs(QUEUE_NAME, "Hello from SQS producer!")
    
    # Example 2: Sending a JSON message
    message = {
        "id": 123456,
        "action": "process_order",
        "url": "https://arstechnica.com/gadgets/2025/03/apple-updates-all-its-operating-systems-brings-apple-intelligence-to-vision-pro/",
        "data": {
            "customer_id": "C789",
            "product_id": "P456",
            "quantity": 2
        }
    }
    push_message_to_sqs(QUEUE_NAME, message)
    
    # Example 3: Sending a message with attributes
    attributes = {
        "priority": "high",
        "source": "inventory_system",
        "timestamp": "2025-03-18T12:00:00Z"
    }
    push_message_to_sqs(QUEUE_NAME, message, attributes)