import os
import pika

f = open("../../benchmarks/sinkOld.txt", "a")

def main():
    """Main entry point to the program."""

    # Get the location of the AMQP broker (RabbitMQ server) from
    # an environment variable
    queue_name = 'pub'

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)


    channel.basic_consume(queue=queue_name,
                        auto_ack=True,
                        on_message_callback=callback)

    print("*** waiting and listening for messages ***")
    channel.start_consuming()

def callback(ch, method, properties, body):
    f.write(body.decode("utf-8") + "\n" )


if __name__ == '__main__':
    main()
