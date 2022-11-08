package io.kokuwa.kafka.messaging;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.kokuwa.kafka.AbstractTest;

/**
 * Test for {@link KafkaMessageListener}.
 *
 * @author Stephan Schnabel
 */
public class KafkaMessageListenerTest extends AbstractTest {

	@DisplayName("minimal message")
	@Test
	void messageMinimal() {

		var topic = TOPIC1;
		var key = UUID.randomUUID().toString();
		var value = UUID.randomUUID().toString();
		send(new ProducerRecord<>(topic, key, value));

		Awaitility.await("message wrote to database").until(() -> repository.count() != 0);

		var message = repository.findAll().iterator().next();
		assertAll("message",
				() -> assertEquals(topic, message.getTopic(), "topic"),
				() -> assertEquals(key, message.getKey(), "key"),
				() -> assertEquals(value, message.getValue(), "value"),
				() -> assertNotNull(message.getTimestamp(), "timestamp"),
				() -> assertEquals(TimestampType.CREATE_TIME, message.getTimestampType(), "timestampType"),
				() -> assertEquals(Map.of(), message.getHeaders(), "headers"));
	}

	@DisplayName("message without value")
	@Test
	void messageWithoutValue() {

		var topic = TOPIC1;
		var key = UUID.randomUUID().toString();
		String value = null;
		send(new ProducerRecord<>(topic, key, value));

		Awaitility.await("message wrote to database").until(() -> repository.count() != 0);

		var message = repository.findAll().iterator().next();
		assertAll("message",
				() -> assertEquals(topic, message.getTopic(), "topic"),
				() -> assertEquals(key, message.getKey(), "key"),
				() -> assertEquals(value, message.getValue(), "value"),
				() -> assertNotNull(message.getTimestamp(), "timestamp"),
				() -> assertEquals(TimestampType.CREATE_TIME, message.getTimestampType(), "timestampType"),
				() -> assertEquals(Map.of(), message.getHeaders(), "headers"));
	}

	@DisplayName("message with header")
	@Test
	void messageWithHeader() {

		var topic = TOPIC2;
		var key = UUID.randomUUID().toString();
		var value = UUID.randomUUID().toString();
		var headers = Set.<Header>of(new RecordHeader("foo", "1".getBytes()), new RecordHeader("bar", "2".getBytes()));
		send(new ProducerRecord<>(topic, null, key, value, headers));

		Awaitility.await("message wrote to database").until(() -> repository.count() != 0);

		var message = repository.findAll().iterator().next();
		assertAll("message",
				() -> assertEquals(topic, message.getTopic(), "topic"),
				() -> assertEquals(key, message.getKey(), "key"),
				() -> assertEquals(value, message.getValue(), "value"),
				() -> assertNotNull(message.getTimestamp(), "timestamp"),
				() -> assertEquals(TimestampType.CREATE_TIME, message.getTimestampType(), "timestampType"),
				() -> assertEquals(Map.of("foo", "1", "bar", "2"), message.getHeaders(), "headers"));
	}

	@DisplayName("multiple messages")
	@Test
	void messageMultiple() {

		var topic1Before = count(TOPIC1);
		var topic2Before = count(TOPIC2);

		var key = UUID.randomUUID().toString();
		var value = UUID.randomUUID().toString();
		send(new ProducerRecord<>(TOPIC1, key, value),
				new ProducerRecord<>(TOPIC2, key, value),
				new ProducerRecord<>(TOPIC1, key, value),
				new ProducerRecord<>(TOPIC2, key, value),
				new ProducerRecord<>(TOPIC1, key, value));

		Awaitility.await("message wrote to database").until(() -> repository.count() == 5);

		assertEquals(3, count(TOPIC1) - topic1Before, "topic 1");
		assertEquals(2, count(TOPIC2) - topic2Before, "topic 2");
	}
}
