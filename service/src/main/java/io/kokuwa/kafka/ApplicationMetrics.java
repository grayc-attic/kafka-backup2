package io.kokuwa.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.kokuwa.kafka.domain.Message;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

/**
 * Metrics wrapper for micrometer.
 *
 * @author Stephan Schnabel
 */
@Singleton
@RequiredArgsConstructor
public class ApplicationMetrics {

	public static final String COUNTER_NAME = "kafka_backup_messages_total";
	public static final String TAG_TOPIC = "topic";

	private final Map<String, Counter> counters = new HashMap<>();
	private final MeterRegistry meterRegistry;

	public void received(Message message) {
		counter(COUNTER_NAME, Set.of(Tag.of(TAG_TOPIC, message.getTopic()))).increment();
	}

	private Counter counter(String counter, Set<Tag> tags) {
		return counters.computeIfAbsent(counter + tags, string -> meterRegistry.counter(counter, tags));
	}
}
