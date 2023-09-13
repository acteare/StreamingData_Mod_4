"""
    This program sends a message from a .csv to a queue on the RabbitMQ server. 

    Author: Amelia Teare
    Date: September 13, 2023

"""

import pika
import sys
import webbrowser
"changes the print statements to logging"
from util_logger import setup_logger
import csv
import time
import logging

logger, logname = setup_logger(__file__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)
FILE_NAME_TASKS = "tasks.csv"
SHOW_OFFER = True # By selecting True, you are ensuring that the admin website automatically opens. To turn this feature off, type "FALSE"

#Changed the custom formula below to automatically open the Admin Website when SHOW_OFFER is True.
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    global SHOW_OFFER
    if SHOW_OFFER:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

#No Changes were made to the custom formula below. 
def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

#Allows you to read from the .csv file selected rather than individually putting in messages or tasks.
def read_from_file(file_name):
    tasks = []
    with open(file_name, "r") as input_file:
        reader = csv.reader(input_file)
        for row in reader:
            if row:
                tasks.append(row[0])
    return tasks

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
   
    # get the tasks from the csv file using the custom function
    tasks = read_from_file(FILE_NAME_TASKS)

    for task in tasks:
        send_message("localhost", "task_queue3", task)
        logger.info(f"Sent: {task} to RabbitMQ.")