import os
import pika
import time
import uuid
import random

def main():
    """Main entry point to the program."""

    # Get the location of the AMQP broker (RabbitMQ server) from
    # an environment variable
    amqp_url = os.environ['AMQP_URL']
    queue_name = os.environ['QUEUE']
    delay = float(os.environ['DELAY'])

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    message_id = 0
    print("*** start sending messages ***")
    print(amqp_url)
    print(queue_name)
    print(delay)
    while(True):
        msg = 'Message %d' % (message_id,)
        message_id += 1
        correlation_id = random.choice(["uuid1", "uuid2"]) # str(uuid.uuid4())
        properties = pika.BasicProperties(correlation_id=correlation_id)
        channel.basic_publish(exchange='',
                        routing_key=queue_name,
                        body=msg,
                        properties=properties)
        time.sleep(delay)
                        
    connection.close()


if __name__ == '__main__':
    main()
