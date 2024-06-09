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
import struct
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


def main():
    #TODO update this
    """
    On 30 second intervals, process 1 row from smoker-temps.csv file, simulating
    live data coming from a smart smoker.  CSV being processed has 4 columns,
    one column marks the time, and the other three each represent one sensor.

    There is one RabbitMQ queue per sensor.  If there is a value present for
    a sensor, a message will be sent to the appropriate queue.  
    """
    # offer rabbitmq admin site on launch
    offer_rabbitmq_admin_site()

    logger.info(f'Attempting to access adoption_data.csv')

    try:
        # access file
        with open("adoption_data.csv", newline='') as csvfile:
            reader = csv.reader(csvfile)

            # Skip header row
            next(reader)

            # Main section of code where data is reviewed and messages are sent.
            for row in reader:

                # assign variables from row
                name = row[0]
                pet_type = row[1]
                breed = row[2]
                age = row[3]
                color = row[4]
                shelter_name = row[5]
                shelter_city = row[6]
                shelter_state = row[7]
                date_posted = row[8]



                logger.info(f'{date_posted} - Row Injested: {name=}, {pet_type=}, {breed=}, {age=}, {color=}, {shelter_name=}, {shelter_city=}, {date_posted=}')

                
                # convert datetime string into a datetime object
                datetime_timestamp = datetime.strptime(string_timestamp, "%m/%d/%y %H:%M:%S").timestamp()
                
                # check for a value present for each sensor (smokerTemp, foodATemp, foodBTemp)
                # if there is a value, send a message to that sensor's queue
                # smoker itself
                if smokerTemp:
                    #TODO: update this
                    logger.info(f"calling send_message('localhost', '01-smoker', message)")

                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(smokerTemp))
                    send_message('localhost', '01-smoker', message)

                # food A
                if foodATemp:
                    logger.info(f"calling send_message('localhost', '02-food-A', message)")

                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(foodATemp))
                    send_message('localhost', '02-food-A', message)

                # food B
                if foodBTemp:
                    logger.info(f"calling send_message('localhost', '03-food-B', message)")
                    
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(foodBTemp))
                    send_message('localhost', '03-food-B', message)

                # wait 30 seconds before reading next row
                time.sleep(30)
                # time.sleep(.05) # for testing

    except Exception as e:
        logger.info(f'ERROR: {e}')

        

# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # specify file path for data source
    file_path = 'smoker-temp.csv'

    # transmit task list
    logger.info(f'Beginning process: {__name__}')
    main()
