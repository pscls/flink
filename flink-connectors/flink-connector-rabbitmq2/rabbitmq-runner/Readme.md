## RabbitMQ Docker Setup

### Structure
This docker setup contains three container. One for RabbitMQ itself and one each for a publisher 
and a subscriber.

The publisher ans subscriber are basic python scripts which can be parameterized in the 
docker-compose.yml file.
- Publisher
    - Delay between generated and published message
    - Queue Name
- Subscriber
    - Queue Name
    
There is also a non-docker subscriber python file. Which receive messages from the docker RabbitMQ
as well. (queue name has to be set manually in the code)


### Execution
#### Full Docker
``docker-compose up --build`` 
#### Non-Docker Subscriber
- install dependency for RabbitMQ: `pip install pika`
- run subscriber (update queue name if necessary): `python consumerWithoutDocker.py`
