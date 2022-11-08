# Kafka Backup

[![License](https://img.shields.io/github/license/kokuwaio/kafka-backup.svg?label=License)](https://github.com/kokuwaio/kafka-backup/blob/main/LICENSE)
[![Build](https://img.shields.io/github/workflow/status/kokuwaio/kafka-backup/CI?label=CI)](https://github.com/kokuwaio/kafka-backup/actions/workflows/ci.yaml?label=CI)

## Features

* backup to postges
* api to reply based on topic/key

## Configuration

|| key || description || default value ||
| kafka-backup.topic-pattern | whitelist pattern for topics | `.*` |
| kafka-backup.consumer.enabled | consumer for backup enabled | `true` |
| kafka-backup.consumer.client-id | consumer client-id | `backup` |
| kafka-backup.consumer.group-id | group client-id | `backup` |
| kafka-backup.replay.enabled | replay with rest enabled | `true` |
| kafka-backup.replay.header | header to set for replay messages | `backup-replay` |
| kafka-backup.replay.batch-size | batch size to read from database | `1000` |

## Open Topics
