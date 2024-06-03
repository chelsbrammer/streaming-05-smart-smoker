"""
    This program reads temperature data from a CSV file and sends the temperature
    data to RabbitMQ queues for further processing
    
    Author: Chelsea Brammer
    Date: June 2, 2024

"""

import pika
import sys
import webbrowser
import csv
from datetime import datetime
import time

from util_logger import setup_logger
logger, logname = setup_logger(__file__)


def offer_rabbitmq_admin_site(show_offer: bool = True):
    """Offer to open the RabbitMQ Admin website"""
    if not show_offer: 
        print("RabbitMQ Admin connection has been turned off.")
        return
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def rabbitmq_connection(host: str, queues: list):
    """
    Establishes a connection to the RabbitMQ server and declares queues.
    Parameters:
    host (str): the host name or IP address of the RabbitMQ server
    queues (list): list of queue names
    Returns:
    conn: the RabbitMQ connection object
    ch: the RabbitMQ channel object
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # delete and declare new durable queues
        for queue_name in queues:
            ch.queue_delete(queue=queue_name)
            ch.queue_declare(queue=queue_name, durable=True)
            return conn, ch
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"{e}")
        sys.exit(1)
                                
def send_message(timestamp: str, temperature: float, queue_name: str):
    """Send a message to RabbitMQ"""
    try:
        conn, ch = rabbitmq_connection(host, queues)
        # create a message tuple
        message = f"{timestamp}, {temperature}"
    
        # publish the message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        logger.info(f"[X] Sent message to queue {queue_name}: {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
    finally: 
        conn.close()
                  
def read_tasks(file_path: str):
    """ Read messages from a CSV file and send them to RabbitMQ"""     
        
    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader) #Skip header row
        for row in reader:
            timestamp = row[0]
            smoker_temp = row[1]       
            food_a_temp = row[2]
            food_b_temp = row[3]
            if smoker_temp != '':
                send_message(timestamp, smoker_temp, '01-smoker')
            if food_a_temp != '':
                send_message(timestamp, food_a_temp, '02-food-A')
            if food_b_temp != '':
                send_message(timestamp, food_b_temp, '03-food-B')
                                         
            time.sleep(30)
               
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin sitepyton
    # Allow user to choose whether they would like to be directed to the Admin site 
    offer_rabbitmq_admin_site(show_offer=False)
    # create variables   
    file_name = 'smoker-temps.csv'
    host = "localhost"
    queues = ["01-smoker", "02-food-A", "03-food-B"]
    #send message to the queue
    read_tasks(file_name)