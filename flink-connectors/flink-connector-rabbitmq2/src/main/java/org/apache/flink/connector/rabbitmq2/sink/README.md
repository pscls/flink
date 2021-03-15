# RabbitMQ Sink

Flink's RabbitMQ connector provides a sink which enables you to publish your stream directly
to a RabbitMQ exchange in three different consistency modes: at-most-once, at-least-once,
and exactly-once. Furthermore, user defined publish options can be used to customize each message
with regard to exchange and publish settings in the RabbitMQ context.
For at-least-once and exactly-once, __Checkpointing__ needs to be enabled.

## Consistency Modes 
With __at-most-once__, the sink will simply take each message and publish it
(with publish options if given) to RabbitMQ. 

For __at-least-once__, the sink needs to guarantee the delivery of messages.
To achieve this, acknowledgements from RabbitMQ are used. 
Generally, new messages are buffered until an acknowledgement arrives.
On a checkpoint, all send but unacknowledged messages are resent to RabbitMQ.
The buffered messages are then stored in the checkpoint to guarantee their future delivery in case
of a failure.
If the resend is triggered before the acknowledgement arrives, or the acknowledgement is lost,
messages will be duplicated inside RabbitMQ.


The __exactly-once-mode__ mode uses a transactional RabbitMQ channel.
All incoming messages are buffered until a checkpoint is triggered. 
On each checkpoint, all messages are
published/committed as one transaction to ensure that they arrive exactly once.
Messages that are a part of a failed transaction are stored in the checkpoint and resent on the next
checkpoint. 
Because the transactional channel is heavyweight, the performance is expected to be lower than that
of the other modes.

## How to use it
```java
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
