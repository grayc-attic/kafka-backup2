CREATE INDEX idx_message_key ON message(key_);
CREATE INDEX idx_message_key_timestamp ON message(key_,timestamp);
ALTER INDEX title_message_topic RENAME TO idx_message_topic;
ALTER INDEX title_message_topic_timestamp RENAME TO idx_message_topic_timestamp;
ALTER INDEX title_message_topic_key RENAME TO idx_message_topic_key;
ALTER INDEX title_message_topic_key_timestamp RENAME TO idx_message_topic_key_timestamp;
