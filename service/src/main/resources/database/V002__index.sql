CREATE INDEX title_message_topic ON message(topic);
CREATE INDEX title_message_topic_timestamp ON message(topic,timestamp);
CREATE INDEX title_message_topic_key ON message(topic,key_);
CREATE INDEX title_message_topic_key_timestamp ON message(topic,key_,timestamp);
