package org.apache.flink.connector.rabbitmq2.source.enumerator;

/**
 * The EnumState is empty because every reader gets assigned the same split. And therefore, no split
 * assignment needs to be remembered.
 *
 * @see RabbitMQSourceEnumerator
 */
public class RabbitMQSourceEnumState {}
