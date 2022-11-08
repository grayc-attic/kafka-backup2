package io.kokuwa.kafka.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka listener for tests.
 *
 * @author Stephan Schnabel
 */
@KafkaListener(clientId = "test", groupId = "test", offsetReset = OffsetReset.EARLIEST)
@Slf4j
public class TestMessageListener {

	@Getter
	private final List<ConsumerRecord<String, String>> records = new ArrayList<>();

	@Topic(patterns = "${kafka-backup.topic-pattern:.*}")
	public void receive(ConsumerRecord<String, String> record) {
		log.info("Retrieved message for {}/{} with headers {}.", record.topic(), record.key(), record.headers());
		records.add(record);
	}

	public void deleteAll() {
		records.clear();
	}
}
