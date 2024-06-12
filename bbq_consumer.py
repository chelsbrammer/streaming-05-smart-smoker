"""
    This program listens for work messages continuously. 
    Start multiple versions to add more workers.  

    Author: Chelsea Brammer
    Date: June 9, 2024

"""

import pika
import sys
from datetime import datetime
from collections import deque

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Deques to store temperature readings with specific maximum lengths
smoker_deque = deque(maxlen=5)  # 2.5 min * 1 reading / 0.5 min (5 readings) 
foodA_deque = deque(maxlen=20)  # 10 min * 1 reading / 0.5 min (20 readings)
foodB_deque = deque(maxlen=20)  # 10 min * 1 reading / 0.5 min (20 readings)

# Define a callback function for smoker queue messages
def smoker_callback(ch, method, properties, body):
    """Define behavior on getting a smoker message."""
    # Split the decoded string into timestamp and temperature components
    timestamp_str, temperature_str = body.decode().split(', ')
    timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M")
    temperature = float(temperature_str)
    
    smoker_deque.append((timestamp, temperature))
    logger.info(f" [x] Received smoker message at {timestamp}: {temperature}")
        
    if len(smoker_deque) == smoker_deque.maxlen:
        if smoker_deque[0][1] - smoker_deque[-1][1] >= 15:
            logger.info(" [!] Alert! Smoker temperature decreased by more than 15 degrees F in 2.5 minutes.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(" [x] Done with smoker message.")
    

# Define a callback function for food A queue messages
def foodA_callback(ch, method, properties, body):
    """Define behavior on getting a food A message."""
    # Split the decoded string into timestamp and temperature components
    timestamp_str, temperature_str = body.decode().split(', ')
    timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M")
    temperature = float(temperature_str)
    
    foodA_deque.append(temperature)
    logger.info(f" [x] Received food A message at {timestamp}: {temperature}")

    if len(foodA_deque) == foodA_deque.maxlen:
        if max(foodA_deque) - min(foodA_deque) <= 1:
            logger.info(" [!] Alert! Food A temperature changed less than 1 degree F in 10 minutes.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(" [x] Done with food A message.")

# Define a callback function for food B queue messages
def foodB_callback(ch, method, properties, body):
    """Define behavior on getting a food B message."""
    # Split the decoded string into timestamp and temperature components
    timestamp_str, temperature_str = body.decode().split(', ')
    timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M")
    temperature = float(temperature_str)
    
    foodB_deque.append(temperature)
    logger.info(f" [x] Received food B message at {timestamp}: {temperature}")

    if len(foodB_deque) == foodB_deque.maxlen:
        if max(foodB_deque) - min(foodB_deque) <= 1:
            logger.info(" [!] Alert! Food B temperature changed less than 1 degree F in 10 minutes.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(" [x] Done with food B message.")

# Define a main function to run the program
def main(hn: str = "localhost", queues: list = ["01-smoker", "02-food-A", "03-food-B"]):
    """Continuously listen for task messages on named queues."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        channel = connection.channel()

        # Declare each queue
        for queue_name in queues:
            channel.queue_declare(queue=queue_name, durable=True)

        channel.basic_qos(prefetch_count=1)

        # Set up basic consume for each queue
        channel.basic_consume(queue="01-smoker", on_message_callback=smoker_callback, auto_ack=False)
        channel.basic_consume(queue="02-food-A", on_message_callback=foodA_callback, auto_ack=False)
        channel.basic_consume(queue="03-food-B", on_message_callback=foodB_callback, auto_ack=False)

        print(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

if __name__ == "__main__":
    main("localhost")