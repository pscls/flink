// package org.apache.flink.connector.rabbitmq2.source.helper;
//
// import com.rabbitmq.client.AMQP;
// import com.rabbitmq.client.Delivery;
// import com.rabbitmq.client.Envelope;
//
// import org.mockito.Mockito;
//
/// **
// * TODO.
// */
// public class DummyDelivery {
//
//	public static final String defaultCorrelationId = "corr1";
//	public static final long defaultDeliveryTag= 13L;
//	public static final String defaultMessage = "message";
//
//	public static Delivery build(String mockedCorrelationId, long mockedDeliveryTag, String
// mockedBody) {
//		Delivery mockedDelivery = Mockito.mock(Delivery.class);
//		Envelope envelope = Mockito.mock(Envelope.class);
//		Mockito.when(envelope.getDeliveryTag()).thenReturn(mockedDeliveryTag);
//
//		AMQP.BasicProperties properties = Mockito.mock(AMQP.BasicProperties.class);
//		Mockito.when(properties.getCorrelationId()).thenReturn(mockedCorrelationId);
//
//		Mockito.when(mockedDelivery.getEnvelope()).thenReturn(envelope);
//		Mockito.when(mockedDelivery.getProperties()).thenReturn(properties);
//		Mockito.when(mockedDelivery.getBody()).thenReturn(mockedBody.getBytes());
//
//		return mockedDelivery;
//	}
//
//	public static Delivery build() {
//		return DummyDelivery.build(DummyDelivery.defaultCorrelationId, DummyDelivery.defaultDeliveryTag,
// DummyDelivery.defaultMessage);
//	}
//
//
// }
