package io.kokuwa.kafka.replay.infrastructure;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import lombok.Getter;

/**
 * Kafka listener for tests.
 *
 * @author Stephan Schnabel
 */
@KafkaListener(clientId = "test", groupId = "test", offsetReset = OffsetReset.EARLIEST)
public class TestMessageListener {

	@Getter
	private final List<ConsumerRecord<String, String>> records = new ArrayList<>();

	@Topic(patterns = ".*")
	public void receive(ConsumerRecord<String, String> record) {
		records.add(record);
	}

	public void deleteAll() {
		records.clear();
	}
}
