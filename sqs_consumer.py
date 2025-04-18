import boto3
import json
import logging
import time
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_queue_url(queue_name, create_if_not_exists=True):
    """
    Get the URL for an SQS queue, optionally creating it if it doesn't exist
    
    Parameters:
    queue_name (str): The name of the SQS queue
    create_if_not_exists (bool): Whether to create the queue if it doesn't exist
    
    Returns:
    str: The queue URL
    """
    # Initialize SQS client with specific region
    region_name = 'us-east-2'  
    sqs = boto3.client('sqs', region_name=region_name)
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        logger.info(f"Found existing queue: {queue_name}")
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue' and create_if_not_exists:
            logger.info(f"Queue {queue_name} does not exist. Creating it...")
            response = sqs.create_queue(QueueName=queue_name)
            logger.info(f"Successfully created queue: {queue_name}")
            return response['QueueUrl']
        else:
            logger.error(f"Error getting queue URL: {e}")
            raise

def process_message(message):
    """
    Process an SQS message (implement your business logic here)
    
    Parameters:
    message (dict): The SQS message object
    """
    message_id = message.get('MessageId', 'unknown')
    receipt_handle = message.get('ReceiptHandle')
    
    try:
        # Extract the message body
        body = message.get('Body', '{}')
        
        # If the body is JSON, parse it
        try:
            body = json.loads(body)
            logger.info(f"Processing JSON message: {message_id}")
        except json.JSONDecodeError:
            logger.info(f"Processing text message: {message_id}")
        
        # Extract message attributes if any
        attributes = message.get('MessageAttributes', {})
        attribute_values = {k: v.get('StringValue') for k, v in attributes.items()}
        
        if attribute_values:
            logger.info(f"Message attributes: {attribute_values}")
        
        # *** Implement your business logic here ***
        # This is where you would process the message according to your requirements
        logger.info(f"Message body: {body}")
        
        # Simulate processing time
        time.sleep(0.5)
        
        logger.info(f"Successfully processed message: {message_id}")
        return True, receipt_handle
    
    except Exception as e:
        logger.error(f"Error processing message {message_id}: {e}")
        return False, receipt_handle

def delete_message(queue_url, receipt_handle):
    """Delete a message from the queue after processing"""
    # Initialize SQS client with specific region
    region_name = 'us-east-2'  # Replace with your actual region like 'us-east-1'
    sqs = boto3.client('sqs', region_name=region_name)
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        return True
    except ClientError as e:
        logger.error(f"Error deleting message: {e}")
        return False

def consume_messages(queue_name, max_messages=10, wait_time=20, visibility_timeout=30):
    """
    Consume and process messages from an SQS queue
    
    Parameters:
    queue_name (str): The name of the SQS queue
    max_messages (int): Maximum number of messages to retrieve at once (1-10)
    wait_time (int): Long polling wait time in seconds (0-20)
    visibility_timeout (int): How long messages are invisible after being received
    """
    # Get queue URL
    queue_url = get_queue_url(queue_name)
    logger.info(f"Consuming messages from {queue_name}")
    
    # Initialize SQS client with specific region
    region_name = 'us-east-2'  # Replace with your actual region like 'us-east-1'
    sqs = boto3.client('sqs', region_name=region_name)
    
    while True:
        try:
            # Receive messages from the queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            if not messages:
                logger.info("No messages received. Continuing to poll...")
                continue
            
            logger.info(f"Received {len(messages)} messages")
            
            # Process each message
            for message in messages:
                success, receipt_handle = process_message(message)
                
                # Delete the message if processing was successful
                if success and receipt_handle:
                    deleted = delete_message(queue_url, receipt_handle)
                    if deleted:
                        logger.info(f"Deleted message {message.get('MessageId')}")
                    else:
                        logger.warning(f"Failed to delete message {message.get('MessageId')}")

            return
        
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            break
        
        except Exception as e:
            logger.error(f"Error in message consumption loop: {e}")
            # Add a small delay before retrying
            time.sleep(5)

if __name__ == "__main__":
    # Configuration
    QUEUE_NAME = 'test-queue'
    MAX_MESSAGES = 5  # Number of messages to retrieve in one batch
    WAIT_TIME = 20    # Long polling time in seconds
    VISIBILITY_TIMEOUT = 60  # Time in seconds that messages are hidden after being received
    
    # Start consuming messages
    logger.info(f"Starting SQS consumer for queue {QUEUE_NAME}")
    consume_messages(
        queue_name=QUEUE_NAME,
        max_messages=MAX_MESSAGES,
        wait_time=WAIT_TIME,
        visibility_timeout=VISIBILITY_TIMEOUT
    )