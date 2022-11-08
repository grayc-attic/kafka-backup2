package io.kokuwa.kafka.messaging;

import java.time.Instant;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import io.kokuwa.kafka.ApplicationMetrics;
import io.kokuwa.kafka.ApplicationProperties;
import io.kokuwa.kafka.domain.Message;
import io.kokuwa.kafka.domain.MessageRepository;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka listener to store payloads in database.
 *
 * @author Stephan Schnabel
 */
@Requires(property = "kafka-backup.consumer.enabled", notEquals = "false", defaultValue = "true")
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
@Slf4j
@RequiredArgsConstructor
public class KafkaMessageListener {

	private final MessageRepository repository;
	private final ApplicationMetrics metrics;
	private final ApplicationProperties properties;

	@Topic(patterns = "${kafka-backup.topic-pattern:.*}")
	public void receive(ConsumerRecord<String, String> record) {

		var topic = record.topic();
		var key = record.key();

		MDC.put("topic", topic);
		MDC.put("key", key);
		try {

			if (record.headers().lastHeader(properties.getHeader()) != null) {
				log.trace("Ignored replayed message for {}/{}.", topic, key);
				return;
			}

			var message = new Message()
					.setTopic(topic)
					.setKey(key)
					.setValue(record.value())
					.setTimestamp(Instant.ofEpochMilli(record.timestamp()))
					.setTimestampType(record.timestampType())
					.setHeaders(new HashMap<>());
			record.headers().forEach(header -> message.getHeaders().put(header.key(), new String(header.value())));

			repository.save(message);
			metrics.received(message);
			log.trace("Stored message for {}/{}.", topic, key);

		} finally {
			MDC.remove("topic");
			MDC.remove("key");
		}
	}
}
