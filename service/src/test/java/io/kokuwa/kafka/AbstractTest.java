package io.kokuwa.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.kokuwa.kafka.rest.JwtProvider;
import io.kokuwa.kafka.test.TestMessageListener;
import io.kokuwa.kafka.test.TestMessageRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.security.token.jwt.signature.SignatureGeneratorConfiguration;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;

/**
 * Base for all unit tests.
 *
 * @author Stephan Schnabel
 */
@MicronautTest(transactional = false, environments = "h2")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.DisplayName.class)
public abstract class AbstractTest implements TestPropertyProvider {

	public static final String TOPIC1 = "topic.abc";
	public static final String TOPIC2 = "topic.def";

	@Inject
	public TestMessageRepository repository;
	@Inject
	public TestMessageListener listener;
	@Inject
	Producer<String, String> kafkaProducer;
	@Inject
	MeterRegistry meterRegistry;
	@Inject
	SignatureGeneratorConfiguration signature;

	// test setup

	@BeforeEach
	void setUp() {
		repository.deleteAll();
		listener.deleteAll();
	}

	@Override
	public Map<String, String> getProperties() {
		Awaitility.setDefaultTimeout(Duration.ofSeconds(10));
		Awaitility.setDefaultPollInterval(Duration.ofMillis(100));
		var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.4"));
		kafka.start();
		return Map.of(
				"kafka-backup.replay.batch-size", "2",
				"kafka.bootstrap.servers", kafka.getBootstrapServers(),
				"kafka.consumers.default.metadata.max.age.ms", "200");
	}

	// helper

	public String auth() {
		return new JwtProvider(signature).bearer("subject");
	}

	public int count(String topic) {
		return (int) meterRegistry
				.counter(ApplicationMetrics.COUNTER_NAME, ApplicationMetrics.TAG_TOPIC, topic)
				.count();
	}

	@SafeVarargs
	public final void send(ProducerRecord<String, String>... records) {
		var futures = Stream.of(records).map(kafkaProducer::send).toList();
		Awaitility.await("producer records send to kafka").until(() -> futures.stream().allMatch(Future::isDone));
	}
}
