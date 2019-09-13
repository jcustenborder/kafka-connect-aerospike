/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class AerospikeSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(AerospikeSinkTask.class);
  private AerospikeClientFactory aerospikeClientFactory = new AerospikeClientFactoryImpl();
  private AerospikeConnectorConfig config;
  private IAerospikeClient aerospikeClient;
  private AerospikeKeyConverter keyConverter;
  private AerospikeValueConverter valueConverter;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new AerospikeConnectorConfig(settings);
    this.keyConverter = new AerospikeKeyConverter(this.config.namespace);
    this.valueConverter = new AerospikeValueConverter();
    this.aerospikeClient = this.aerospikeClientFactory.create(this.config);
    log.info("start() - Retrieving previously written offsets from Aerospike.");
    Key[] partitionAssignments = this.context.assignment()
        .stream()
        .map(this::topicPartitionKey)
        .toArray(Key[]::new);
    Record[] records = this.aerospikeClient.get(null, partitionAssignments);
    Map<TopicPartition, Long> requestedOffsets = new LinkedHashMap<>();
    for (Record record : records) {
      if (null != record) {
        String topic = record.getString("topic");
        int partition = record.getInt("partition");
        long offset = record.getLong("offset");
        requestedOffsets.put(new TopicPartition(topic, partition), offset);
      }
    }
    if (!requestedOffsets.isEmpty()) {
      log.info("start() - Requesting previously committed offsets {}.", requestedOffsets);
      this.context.offset(requestedOffsets);
    } else {
      log.info("start() - No Previous offsets found.");
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      final Key key = this.keyConverter.convert(record);
      if (null != record.value()) {
        Bin[] value = this.valueConverter.convert(record.value()).toArray(new Bin[0]);
        log.trace("put() - Calling put(null, '{}', '{}')", key, value);
        this.aerospikeClient.put(null, key, value);
      } else {
        log.trace("put() - Calling delete(null, '{}')", key);
        this.aerospikeClient.delete(null, key);
      }
    }
  }

  /**
   * Method is used to build a key to our namespace for the given Topic Partition.
   *
   * @param topicPartition
   * @return
   */
  Key topicPartitionKey(TopicPartition topicPartition) {
    final String key = topicPartition.topic() + "-" + topicPartition.partition();
    return new Key(this.config.namespace, this.config.offsetSetName, key);
  }


  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    log.info("flush() - Flushing offsets for '{}'", this.config.offsetSetName);

    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
      final Key key = topicPartitionKey(entry.getKey());
      final long offset = entry.getValue().offset();
      log.trace("flush() - Setting offset for {} to {}", entry.getKey(), offset);
      Bin[] bins = new Bin[]{
          new Bin("topic", entry.getKey().topic()),
          new Bin("partition", entry.getKey().partition()),
          new Bin("offset", offset)
      };
      this.aerospikeClient.put(null, key, bins);
    }
  }

  @Override
  public void stop() {
    if (null != this.aerospikeClient) {
      log.info("stop() - Closing Aerospike Client.");
      this.aerospikeClient.close();
    }
  }
}
