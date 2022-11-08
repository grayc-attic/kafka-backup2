package io.kokuwa.kafka.replay.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.kokuwa.kafka.replay.infrastructure.AbstractIT;

/**
 * Usecase: backup to database.
 *
 * @author Stephan Schnabel
 */
@DisplayName("Backup")
public class BackupIT extends AbstractIT {

	@DisplayName("single message")
	@Test
	void singleMessage() {

		var topic = UUID.randomUUID().toString().substring(4);
		var key = UUID.randomUUID().toString();
		var value = UUID.randomUUID().toString();
		var headers = Set.<Header>of(new RecordHeader("foo", "1".getBytes()), new RecordHeader("bar", "2".getBytes()));

		var beforeTotal = prometheusClient.scrapMessages();
		var beforeTopic = prometheusClient.scrapMessages(topic);

		listener.deleteAll();
		var future = kafkaProducer.send(new ProducerRecord<>(topic, null, key, value, headers));
		Awaitility.await("kafka message send").until(future::isDone);
		Awaitility.await("wait for listener").until(() -> listener.getRecords().size() == 1);
		Awaitility.await("wait for topic " + topic).until(() -> prometheusClient.scrapMessages() > beforeTotal);

		assertEquals(1, prometheusClient.scrapMessages() - beforeTotal, "total");
		assertEquals(1, prometheusClient.scrapMessages(topic) - beforeTopic, "topic");
	}
}
