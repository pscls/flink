import os
import pika
import time

def main():
    """Main entry point to the program."""

    # Get the location of the AMQP broker (RabbitMQ server) from
    # an environment variable
    amqp_url = os.environ['AMQP_URL']
    queue_name = os.environ['QUEUE']
    delay = float(os.environ['DELAY'])

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

    message_id = 0
    print("*** start sending messages ***")
    print(amqp_url)
    print(queue_name)
    print(delay)
    while(True):
        msg = 'Message %d' % (message_id,)
        message_id += 1
        channel.basic_publish(exchange='',
                        routing_key=queue_name,
                        body=msg)
        time.sleep(delay)
                        
    connection.close()


if __name__ == '__main__':
    main()
