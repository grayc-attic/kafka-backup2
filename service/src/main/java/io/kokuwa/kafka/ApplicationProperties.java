package io.kokuwa.kafka;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Properties for applicatinn.
 *
 * @author Stephan Schnabel
 */
@ConfigurationProperties("kafka-backup.replay")
@Getter
@Setter
public class ApplicationProperties {

	private String header = "backup-replay";
	private int batchSize = 1000;
}
