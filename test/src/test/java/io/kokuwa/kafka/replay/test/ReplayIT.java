package io.kokuwa.kafka.replay.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.kokuwa.kafka.replay.infrastructure.AbstractIT;
import io.kokuwa.kafka.rest.KafkaApiClient;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Inject;

/**
 * Usecase: replay backuped messages.
 *
 * @author Stephan Schnabel
 */
@DisplayName("Replay")
public class ReplayIT extends AbstractIT {

	private static final List<ProducerRecord<String, String>> RECORDS = new ArrayList<>();
	private static final String TOPIC = UUID.randomUUID().toString().substring(10);
	private static final String KEY = UUID.randomUUID().toString().substring(10);
	private static final Instant FROM = Instant.now().minus(Duration.ofDays(600));
	private static final Instant TO = FROM.plus(Duration.ofDays(500));

	@Inject
	KafkaApiClient client;

	@DisplayName("1. create messages")
	@Test
	void createMessages() {
		IntStream.range(1, 1111)
				.mapToObj(i -> new ProducerRecord<>(TOPIC, null, FROM.plus(Duration.ofHours(i)).toEpochMilli(), KEY,
						UUID.randomUUID().toString()))
				.forEach(RECORDS::add);
		RECORDS.add(new ProducerRecord<>(TOPIC, null, FROM.minus(Duration.ofDays(1)).toEpochMilli(), KEY, "before"));
		RECORDS.add(new ProducerRecord<>(TOPIC, null, TO.plus(Duration.ofDays(1)).toEpochMilli(), KEY, "after"));
	}

	@DisplayName("2. backup messages")
	@Test
	void backupMessages() {
		listener.deleteAll();
		var futures = RECORDS.stream().map(kafkaProducer::send).toList();
		Awaitility.await("kafka message sent").until(() -> futures.stream().allMatch(Future::isDone));
		Awaitility.await("wait for listener").until(() -> listener.getRecords().size() == RECORDS.size());
		Awaitility.await("wait for backup").until(() -> prometheusClient.scrapMessages(TOPIC) == RECORDS.size());
	}

	@DisplayName("3. replay messages")
	@Test
	void replayMessages() {
		listener.deleteAll();
		var keys = assertStatus(HttpStatus.OK, () -> client.getKeys(TOPIC)).body();
		assertEquals(List.of(KEY), keys, "keys");
		assertStatus(HttpStatus.NO_CONTENT, () -> client.replayTopicKey(TOPIC, KEY, FROM, TO));
	}

	@DisplayName("4. check replay")
	@Test
	void check() {
		Awaitility.await("wait for replay").until(() -> listener.getRecords().size() == RECORDS.size() - 2);
		Awaitility.await("wait for backup").until(() -> prometheusClient.scrapMessages(TOPIC) == RECORDS.size());
	}
}
