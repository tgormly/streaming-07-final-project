Tim Gormly
6/9/2024

## Steaming Data - Final Project

### Data source
This project will simulate a stream of data of new adoption listings for animals at various animal shelters troughout the United States. This data will be generated as a CSV that will be read by a producer. 

### Streamed Data
Each animal listing will be sent by the producer, and different listening workers will receive and act upon this data. When creating listening workers, the user will be able to specify what type of animal they are interested in. The worker will generate an alert when there is a new animal available that matches the users' requirements. 

<hr>


## Requirements
A valid Python environment is required.  Faker and pika libraries are used. 

This repo was written in Python 3.11.9.

RabbitMQ services will need to be active.  Additional information on RabbitMQ can be found here: https://www.rabbitmq.com/tutorials

<hr>

### Executing Code
Initialize RabbitMQ services

Run <code>generate_data.py</code> to generate simulated adoption data.

In one console, run <code>producer.py</code> This will begin sending messages to RabbitMQ
![Console output of Producer](/images/Producer_Console.png)

In another console, run <code>consumer.py</code> This will receive and process messages from RabbitMQ
![Console output of Consumer](/images/Consumer_Console.png)