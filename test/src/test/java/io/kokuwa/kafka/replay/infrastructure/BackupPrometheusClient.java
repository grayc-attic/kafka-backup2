package io.kokuwa.kafka.replay.infrastructure;

import io.micronaut.http.client.annotation.Client;

/**
 * Prometheus clients for kafka backup.
 *
 * @author Stephan Schnabel
 */
@Client("management")
public interface BackupPrometheusClient extends PrometheusClient {

	default long scrapMessages() {
		return scrapCounter("kafka_backup_messages");
	}

	default long scrapMessages(String topic) {
		return scrapCounter("kafka_backup_messages", "topic", topic);
	}
}
