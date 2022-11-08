package io.kokuwa.kafka.rest;

import static io.kokuwa.kafka.rest.HttpResponseAssertions.assert200;
import static io.kokuwa.kafka.rest.HttpResponseAssertions.assert204;
import static io.kokuwa.kafka.rest.HttpResponseAssertions.assert401;
import static io.kokuwa.kafka.rest.HttpResponseAssertions.assert404;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.kokuwa.kafka.AbstractTest;
import jakarta.inject.Inject;

/**
 * Test for {@link KafkaApi}.
 *
 * @author Stephan Schnabel
 */
public class KafkaApiTest extends AbstractTest implements KafkaApiTestSpec {

	@Inject
	KafkaApiTestClient client;

	@DisplayName("getTopics(200): no topics")
	@Test
	@Override
	public void getTopics200() {
		var topics = assert200(() -> client.getTopics(auth())).body();
		assertEquals(List.of(), topics, "topics");
	}

	@DisplayName("getTopics(200): multiple topics")
	@Test
	public void getTopics200Found() {
		repository.save("b", UUID.randomUUID().toString());
		repository.save("a", UUID.randomUUID().toString());
		repository.save("c", UUID.randomUUID().toString());
		var topics = assert200(() -> client.getTopics(auth())).body();
		assertEquals(List.of("a", "b", "c"), topics, "topics");
	}

	@DisplayName("getTopics(401): unauthorized")
	@Test
	@Override
	public void getTopics401() {
		assert401(() -> client.getTopics(null));
	}

	@DisplayName("getKeys(200): keys found")
	@Test
	@Override
	public void getKeys200() {
		repository.save("t1", "b");
		repository.save("t1", "a");
		repository.save("t1", "c");
		repository.save("t2", "d");
		var keys = assert200(() -> client.getKeys(auth(), "t1")).body();
		assertEquals(List.of("a", "b", "c"), keys, "keys");
	}

	@DisplayName("getKeys(401): unauthorized")
	@Test
	@Override
	public void getKeys401() {
		assert401(() -> client.getKeys(null, "nope"));
	}

	@DisplayName("getKeys(404): topic not found")
	@Test
	@Override
	public void getKeys404() {
		assert404(() -> client.getKeys(auth(), "nope"));
	}

	@DisplayName("replayTopicKey(204): replayed")
	@Test
	@Override
	public void replayTopic204() {

		// create test data

		repository.save("t1", "k1");
		repository.save("t1", "k1");
		repository.save("t1", "k1");
		repository.save("t1", "k2");
		repository.save("t1", "k2");
		repository.save("t2", "k3");

		// trigger replay

		var countBefore = count("t1");
		assert204(() -> client.replayTopic(auth(), "t1", null, null));
		Awaitility.await("messages replayed").until(() -> listener.getRecords().size() == 5);
		assertEquals(countBefore, count("t1"), "metrics count");
		assertEquals(6, repository.count(), "repository count");

		// check state

		for (var record : listener.getRecords()) {
			assertEquals("t1", record.topic(), "topic");
			assertNotNull(record.headers().lastHeader("backup-replay"), "header");
		}
	}

	@DisplayName("replayTopic(401): unauthorized")
	@Test
	@Override
	public void replayTopic401() {
		assert401(() -> client.replayTopic(null, "nope", null, null));
	}

	@DisplayName("replayTopic(404): topic not found")
	@Test
	@Override
	public void replayTopic404() {
		assert404(() -> client.replayTopic(auth(), "nope", null, null));
	}

	@DisplayName("replayTopicKey(204): replayed")
	@Test
	@Override
	public void replayTopicKey204() {

		// create test data

		var message = repository.save("t1", "k1", Map.of("foo", "bar"));
		repository.save("t1", "k2");
		repository.save("t2", "k1");

		// trigger replay

		var countBefore = count("t1");
		assert204(() -> client.replayTopicKey(auth(), "t1", "k1", null, null));
		Awaitility.await("messages replayed").until(() -> listener.getRecords().size() == 1);
		assertEquals(countBefore, count("t1"), "metrics count");
		assertEquals(3, repository.count(), "repository count");

		// check state

		var record = listener.getRecords().get(0);
		assertEquals(message.getTopic(), record.topic(), "topic");
		assertEquals(message.getKey(), record.key(), "key");
		assertEquals(message.getValue(), record.value(), "value");
		assertEquals("bar", new String(record.headers().lastHeader("foo").value()), "header foo");
		assertNotNull(record.headers().lastHeader("backup-replay"), "header replay");
	}

	@DisplayName("replayTopicKey(401): unauthorized")
	@Test
	@Override
	public void replayTopicKey401() {
		assert401(() -> client.replayTopicKey(null, "nope", "nope", null, null));
	}

	@DisplayName("replayTopicKey(404): topic not found")
	@Test
	@Override
	public void replayTopicKey404() {
		assert404(() -> client.replayTopicKey(auth(), "nope", "nope", null, null));
	}

	@DisplayName("replayTopicKey(404): key not found")
	@Test
	public void replayTopicKey404Key() {
		repository.save("t1", "k1");
		repository.save("t2", "k2");
		assert404(() -> client.replayTopicKey(auth(), "t1", "k2", null, null));
	}
}
