import os
import pika

def main():
    """Main entry point to the program."""

    # Get the location of the AMQP broker (RabbitMQ server) from
    # an environment variable
    queue_name = 'pub'

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

    
    channel.basic_consume(queue=queue_name,
                        auto_ack=True,
                        on_message_callback=callback)
                        
    print("*** waiting and listening for messages ***")
    channel.start_consuming()

def callback(ch, method, properties, body):
    print("== Received: %r" % body)


if __name__ == '__main__':
    main()
