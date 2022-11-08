package io.kokuwa.kafka.test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.record.TimestampType;

import io.kokuwa.kafka.domain.Message;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.repository.GenericRepository;

/**
 * Repository for {@link Message} in tests.
 *
 * @author Stephan Schnabel
 */
@JdbcRepository
public interface TestMessageRepository extends GenericRepository<Message, Long> {

	List<Message> findAll();

	int count();

	Message save(Message message);

	default Message save(String topic, String key) {
		return save(topic, key, Map.of());
	}

	default Message save(String topic, String key, Map<String, String> header) {
		return save(new Message()
				.setTopic(topic)
				.setKey(key)
				.setValue(UUID.randomUUID().toString())
				.setHeaders(header)
				.setTimestamp(Instant.now())
				.setTimestampType(TimestampType.CREATE_TIME));
	}

	void deleteAll();
}
