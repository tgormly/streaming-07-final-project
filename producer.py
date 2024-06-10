"""
Tim Gormly
6/9/2024

This program will send messages to RabbitMQ.  These messages are meant to simulate
new-listing updates from animal shelters across the United States.  These will be
consumed downstream by consumers listening to the RabbitMQ queues.

"""

import pika
import sys
import webbrowser
import csv
import pickle
import time 
from datetime import datetime
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# use to control whether or not admin page is offered to user.
# change to true to receive offer, false to remove prompt
show_offer = True

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

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
        logger.info(f"send_message({host=}, {queue_name=}, {message=})")
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()
        logger.info(f"connection opened: {host=}, {queue_name=}")

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)

        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)

        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()
        logger.info(f"connection closed: {host=}, {queue_name=}")


def main(filepath):
    """
    On 30 second intervals, process 1 row from adoption_data.csv file, simulating
    live data coming from animal shelters.  CSV being processed has 8 columns,
    one column marks the time, and the others are different attributes of the animal
    available for adoption

    There is one RabbitMQ queue per animal type.  Cats and Dogs will each be
    sent to their own queues.
    """
    # offer rabbitmq admin site on launch
    offer_rabbitmq_admin_site()

    logger.info(f'Attempting to access {filepath}')

    try:
        # access file
        with open(filepath, newline='') as csvfile:
            reader = csv.reader(csvfile)

            # Skip header row
            next(reader)

            # Main section of code where data is reviewed and messages are sent.
            for row in reader:

                # assign variables from row
                animal_data = {
                    'name': row[0],
                    'pet_type': row[1],
                    'breed': row[2],
                    'age': int(row[3]),
                    'color': row[4],
                    'shelter_name': row[5],
                    'shelter_city': row[6],
                    'shelter_state': row[7],
                    'date_posted': row[8]
                }
                logger.info(f"{animal_data['date_posted']} - Row Injested: "
                    f"{animal_data['name']}, {animal_data['pet_type']}, {animal_data['breed']}, "
                    f"{animal_data['age']}, {animal_data['color']}, {animal_data['shelter_name']}, "
                    f"{animal_data['shelter_city']}, {animal_data['shelter_state']}, {animal_data['date_posted']}")
                
                
                # select queue depending on pet type
                queue = 'new-' + animal_data['pet_type'] + 's'
                
                logger.info(f"calling send_message('localhost', {queue}, {animal_data['name']}, {animal_data['pet_type']}, {animal_data['breed']})")

                # pack message contents into serialized format
                message = pickle.dumps(animal_data)
                send_message('localhost', queue, message)

                # wait 30 seconds before reading next row
                time.sleep(30)
                # time.sleep(.05) # for testing

    except Exception as e:
        logger.info(f'ERROR: {e}')

        

# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # specify file path for data source
    file_path = 'adoption_data.csv'

    # transmit task list
    logger.info(f'Beginning process: {__name__}')
    main(file_path)
