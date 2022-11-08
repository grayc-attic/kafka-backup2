package io.kokuwa.kafka.messaging;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import io.kokuwa.kafka.ApplicationProperties;
import io.kokuwa.kafka.domain.Message;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

/**
 * Kafka message producer
 *
 * @author Stephan Schnabel
 */
@Singleton
@RequiredArgsConstructor
public class KafkaMessageProducer {

	private final ApplicationProperties properties;
	private final Producer<String, String> kafka;

	public Future<RecordMetadata> send(Message message) {
		return kafka.send(toRecord(message));
	}

	private ProducerRecord<String, String> toRecord(Message message) {

		var headers = new ArrayList<Header>();
		headers.add(new RecordHeader(properties.getHeader(), Boolean.TRUE.toString().getBytes()));
		message.getHeaders().forEach((k, v) -> headers.add(new RecordHeader(k, v.getBytes())));

		return new ProducerRecord<>(
				message.getTopic(),
				null,
				message.getTimestamp().toEpochMilli(),
				message.getKey(),
				message.getValue(),
				headers);
	}
}
