# License of the Rabbit MQ Connector

Flink's RabbitMQ connector defines a Maven dependency on the
"RabbitMQ AMQP Java Client", is triple-licensed under the Mozilla Public License 1.1 ("MPL"), 
the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

Flink itself neither reuses source code from the "RabbitMQ AMQP Java Client"
nor packages binaries from the "RabbitMQ AMQP Java Client".

Users that create and publish derivative work based on Flink's
RabbitMQ connector (thereby re-distributing the "RabbitMQ AMQP Java Client")
must be aware that this may be subject to conditions declared in the 
Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL")
and the Apache License version 2 ("ASL").


## Sink

Flink's RabbitMQ connector provides a sink which enables you to publish you stream directly
to a RabbitMQ exchange in three different consistency modes: at-most-once, at-least-once, 
and exactly-once. Furthermore, user defined publish options can be used to customize each message 
options in regards to exchange and publish settings in the RabbitMQ context.



```
RabbitMQSink<T> sink =
                RabbitMQSink.<T>builder()
                        .setConnectionConfig(<RMQConnectionConfig>)
                        .setQueueName(<RabbitMQ Queue Name>)
                        .setSerializationSchema(<Serialization Schema>)
                        .setConsistencyMode(<ConsistencyMode>)
                        .build();

// ******************* An example usage looks like this *******************

RMQConnectionConfig rmqConnectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setVirtualHost("/")
                        .setUserName("guest")
                        .setPassword("guest")
                        .setPort(5672)
                        .build();
                        
RabbitMQSink<String> rmqSink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(rmqConnectionConfig)
                        .setQueueName("publish-queue")
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(ConsistencyMode.AT_MOST_ONCE)
                        .build();

(DataStream<String>).sinkTo(rmqSink)
```
