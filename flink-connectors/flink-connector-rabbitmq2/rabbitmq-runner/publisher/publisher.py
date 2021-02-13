import os
import pika
import time
import uuid
import random

def main():
    """Main entry point to the program."""

    # Get the location of the AMQP broker (RabbitMQ server) from
    # an environment variable
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
#     connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue="pub", durable=True)

    message_id = 0
    print("*** start sending messages ***")
#     print(amqp_url)
#     print(queue_name)
#     print(delay)
    n = 1000000
    defaultMsg = getAvroBytes()
    while(message_id < n):
        if (message_id % 10000 == 0):
            print(message_id)
        msg = 'Message %d' % (message_id,)
        message_id += 1
        correlation_id = random.choice(["uuid1", "uuid2"]) # str(uuid.uuid4())
        properties = pika.BasicProperties(correlation_id=correlation_id)
        channel.basic_publish(exchange='',
                        routing_key="pub",
                        body=defaultMsg,
                        properties=properties)
#         time.sleep(1)

    connection.close()


if __name__ == '__main__':
    main()
