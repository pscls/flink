import os
import pika
import time
import uuid
import random
import io
import avro.schema
import avro.io

test_schema = '''
{
"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "timestamps", "type": "string"}
 ]
}
'''

schema = avro.schema.parse(test_schema)
writer = avro.io.DatumWriter(schema)

bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)

def getAvroBytes():
    writer.write({"timestamps": str(int(time.time() * 1000))}, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes

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
    n = 200000
    msg = getAvroBytes()
    while(message_id < n):
        if (message_id % 10000 == 0):
            print(message_id)
        message_id += 1
        correlation_id = random.choice([str(uuid.uuid4()), str(uuid.uuid4())])
        properties = pika.BasicProperties(correlation_id=correlation_id)
        msg = str(int(time.time() * 1000))
        channel.basic_publish(exchange='',
                        routing_key="pub",
                        body=msg,
                        properties=properties)
#         time.sleep(1)

    connection.close()


if __name__ == '__main__':
    main()
