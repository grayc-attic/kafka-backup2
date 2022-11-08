package io.kokuwa.kafka.rest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import io.kokuwa.kafka.ApplicationProperties;
import io.kokuwa.kafka.domain.Message;
import io.kokuwa.kafka.domain.MessageRepository;
import io.kokuwa.kafka.messaging.KafkaMessageProducer;
import io.micronaut.context.annotation.Requires;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.Pageable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link KafkaApi}.
 *
 * @author Stephan Schnabel
 */
@Requires(property = "kafka-backup.replay.enabled", notEquals = "false", defaultValue = "true")
@Controller
@Slf4j
@RequiredArgsConstructor
public class KafkaController implements KafkaApi {

	private final MessageRepository repository;
	private final KafkaMessageProducer producer;
	private final ApplicationProperties properties;

	@Override
	public HttpResponse<List<String>> getTopics() {
		return HttpResponse.ok(repository.findDistinctTopic());
	}

	@Override
	public HttpResponse<List<String>> getKeys(String topic) {
		return Optional.of(repository.findDistinctKeyByTopic(topic))
				.filter(keys -> !keys.isEmpty())
				.map(HttpResponse::ok)
				.orElseGet(HttpResponse::notFound);
	}

	@Override
	public HttpResponse<Object> replayTopic(String topic, Optional<Instant> from, Optional<Instant> to) {
		if (!repository.findDistinctTopic().contains(topic)) {
			return HttpResponse.notFound();
		}
		replay(topic, null, from, to);
		return HttpResponse.noContent();
	}

	@Override
	public HttpResponse<Object> replayTopicKey(String topic, String key, Optional<Instant> from, Optional<Instant> to) {
		if (!repository.findDistinctKeyByTopic(topic).contains(key)) {
			return HttpResponse.notFound();
		}
		replay(topic, key, from, to);
		return HttpResponse.noContent();
	}

	private void replay(String topic, String key, Optional<Instant> fromOptional, Optional<Instant> toOptional) {

		var batchSize = properties.getBatchSize();
		var to = toOptional.orElseGet(Instant::now);
		var from = fromOptional.orElseGet(() -> to.minus(Duration.ofDays(7)));

		log.info("Start replay {}/{} with batch size {} between {} and {}.", topic, key, batchSize, from, to);

		Pageable pageable = Pageable.from(0, batchSize);
		Page<Message> page = null;

		do {

			page = key == null
					? repository.findByTopicAndTimestampBetweenOrderByTimestamp(topic, from, to, pageable)
					: repository.findByTopicAndKeyAndTimestampBetweenOrderByTimestamp(topic, key, from, to, pageable);
			pageable = page.nextPageable();
			var currentPage = page.getPageNumber() + 1;
			var totalPages = page.getTotalPages();
			var pageSize = page.getContent().size();

			log.debug("Got page {} of {} with {} entries.", currentPage, totalPages, pageSize);
			var futures = page.getContent().stream().map(producer::send).toList();
			for (var future : futures) {
				try {
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					log.error("Replay failed on page {} of {}", currentPage, totalPages, e);
					return;
				}
			}
			log.debug("Sent page {} with {} entries to Kafka.", currentPage, pageSize);

		} while (page.getPageNumber() + 1 < page.getTotalPages());

		log.info("Finished replay {}/{}.", topic, key);
	}
}
