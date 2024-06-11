"""
Tim Gormly
6/10/2024

    This program actively and continuously listens for messages from RabbitMQ queues, simulating new animal listings at animal shelters.
    
    User will specify some criteria they have for animals they are interested in.  This will affect which queue(s) the worker listens to
    (queues are separated by animal type, dog and cat).

    One consumer will listen to one or more queues. If a new animal arrives that matches the users' criteria, an alert will be generated
"""

import pika
import sys
import pickle
from datetime import datetime
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

dog_breeds = [
    'Labrador Retriever', 'German Shepherd', 'Golden Retriever', 'Bulldog', 'Poodle',
    'Beagle', 'Rottweiler', 'Yorkshire Terrier', 'Boxer', 'Dachshund'
]
cat_breeds = [
    'Siamese', 'Persian', 'Maine Coon', 'Ragdoll', 'Sphynx',
    'British Shorthair', 'Abyssinian', 'Birman', 'Oriental Shorthair', 'Scottish Fold'
]

preferred_breed = ''

def display_menu(breeds, pet_type):
    '''Used in user_interest() to display options to users
    no parameters.'''
    print(f"\nSelect a {pet_type} breed from the list below:")
    for index, breed in enumerate(breeds, start=1):
        print(f"{index}. {breed}")

def user_interest():
    '''This function will be used when the worker is initially created. This will grant the user
    the opportunity to specify what sorts of attributes they are interested in.  When a pet
    that matches their interest is received by the worker, they will be notified'''
    while True:
        pet_type = input("Are you interested in a dog or a cat today? ").strip().lower()
        
        if pet_type == 'dog':
            breeds = dog_breeds
            display_menu(breeds, "dog")
        elif pet_type == 'cat':
            breeds = cat_breeds
            display_menu(breeds, "cat")
        else:
            print("Please enter 'dog' or 'cat'.")
            continue

        while True:
            try:
                selection = int(input("Enter the number corresponding to your choice: "))
                if 1 <= selection <= len(breeds):
                    print(f"You have selected: {breeds[selection - 1]}")
                    
                    break
                else:
                    print(f"Please enter a number between 1 and {len(breeds)}.")
            except ValueError:
                print("Invalid input. Please enter a numeric value.")

        # return a tuple containing the pet type the preferred breed.
        preferred_breed = breeds[selection - 1]
        return pet_type, preferred_breed

# define a callback function for each sensor that will be called when a message is received
def new_pet_callback(ch, method, properties, body, preferred_breed):
    """ 
    callback function used when a message is received on a new pets queue
    """
    
    # deserialize the dictionary sent by producer.py
    animal_data = pickle.loads(body)
    
    logger.info(f"{animal_data['date_posted']} - New Animal: \n"
        f"{animal_data['name']}, {animal_data['pet_type']}, {animal_data['breed']}, "
        f"{animal_data['age']}, {animal_data['color']}, {animal_data['shelter_name']}, "
        f"{animal_data['shelter_city']}, {animal_data['shelter_state']}, {animal_data['date_posted']}")


        # when done with task, tell the user
    logger.info(" [New Animal Reading] Done.\n")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for messages on several named queues."""
    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # Declare queues
        queues = ('new-Cats', 'new-Dogs')

        # create durable queue for each queue
        for queue in queues:
            # delete queue if it exists
            channel.queue_delete(queue=queue)
            # create new durable queue
            channel.queue_declare(queue=queue, durable=True)

        # restrict worker to one unread message at a time
        channel.basic_qos(prefetch_count=1) 

        # prompt user for their interest
        pet_type, preferred_breed = user_interest()


        # listen to each queue and execute corresponding callback function
        if pet_type == 'cat':
            channel.basic_consume( queue='new-Cats', on_message_callback=new_pet_callback)
        if pet_type == 'dog':            
            channel.basic_consume( queue='new-Dogs', on_message_callback=new_pet_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
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


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
