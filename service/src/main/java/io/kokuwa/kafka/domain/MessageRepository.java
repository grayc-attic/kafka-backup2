package io.kokuwa.kafka.domain;

import java.time.Instant;
import java.util.List;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.repository.GenericRepository;

/**
 * Repository for {@link Message}.
 *
 * @author Stephan Schnabel
 */
@JdbcRepository
public interface MessageRepository extends GenericRepository<Message, Long> {

	void save(Message message);

	List<String> findDistinctTopic();

	List<String> findDistinctKeyByTopic(String topic);

	Page<Message> findByTopicAndTimestampBetweenOrderByTimestamp(
			String topic,
			Instant from,
			Instant to,
			Pageable pageable);

	Page<Message> findByTopicAndKeyAndTimestampBetweenOrderByTimestamp(
			String topic,
			String key,
			Instant from,
			Instant to,
			Pageable pageable);
}
