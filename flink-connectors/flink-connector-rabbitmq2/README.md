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


# Connector

This connector provides a [source](src/main/java/org/apache/flink/connector/rabbitmq2/source/README.md) 
and [sink](src/main/java/org/apache/flink/connector/rabbitmq2/sink/README.md) for consuming from and
publishing message to RabbitMQ in either an at-most-once, at-least-once, or exactly-once manner.
Further details on the behaviour and data flow can be found in the respective directories.
