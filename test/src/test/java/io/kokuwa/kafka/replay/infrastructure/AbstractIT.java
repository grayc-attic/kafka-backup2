package io.kokuwa.kafka.replay.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Producer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

/**
 * Base for all integration tests.
 *
 * @author Stephan Schnabel
 */
@MicronautTest
@TestMethodOrder(MethodOrderer.DisplayName.class)
public abstract class AbstractIT {

	static {
		Awaitility.setDefaultTimeout(Duration.ofSeconds(30));
		Awaitility.setDefaultPollInterval(Duration.ofMillis(100));
	}

	@Inject
	public Producer<String, String> kafkaProducer;
	@Inject
	public BackupPrometheusClient prometheusClient;
	@Inject
	public TestMessageListener listener;

	public static <T> HttpResponse<T> assertStatus(HttpStatus status, Supplier<HttpResponse<T>> executeable) {
		HttpResponse<T> response = executeable.get();
		assertEquals(status, response.getStatus(), "invalid status");
		return response;
	}
}
