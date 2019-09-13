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
import com.github.jcustenborder.docker.junit5.Port;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.StructHelper.struct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public abstract class AbstractAerospikeSinkTaskIT {
  protected static final String NAMESPACE = "test";
  private static final Logger log = LoggerFactory.getLogger(AbstractAerospikeSinkTaskIT.class);
  protected AerospikeSinkTask task;
  protected SinkTaskContext sinkTaskContext;
  protected Map<String, String> settings;
  protected MySinkRecordHelper recordHelper;
  protected IAerospikeClient aerospikeClient;

  protected abstract IAerospikeClient client(InetSocketAddress address);

  @BeforeEach
  public void start(@Port(container = "aerospike", internalPort = 3000) InetSocketAddress address) {
    this.sinkTaskContext = mock(SinkTaskContext.class);
    this.task = new AerospikeSinkTask();
    this.task.initialize(this.sinkTaskContext);
    this.settings = new LinkedHashMap<>();
    this.settings.put(AerospikeConnectorConfig.NAMESPACE_CONFIG, NAMESPACE);
    this.recordHelper = new MySinkRecordHelper();

    this.aerospikeClient = client(address);

    this.settings.put(AerospikeConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()));
  }


  @Test
  public void offsetsExist() {

    Map<TopicPartition, Long> expectedOffsets = new LinkedHashMap<>();
    for (int i = 0; i < 5; i++) {
      TopicPartition topicPartition = new TopicPartition("offsetsExist", i);
      final long offset = 123421L;
      expectedOffsets.put(topicPartition, offset);
      Key key = new Key(NAMESPACE, AerospikeConnectorConfig.DEFAULT_CONNECT_OFFSET_SET, topicPartition.toString());
      Bin[] bins = new Bin[]{
          new Bin("topic", topicPartition.topic()),
          new Bin("partition", topicPartition.partition()),
          new Bin("offset", offset)
      };
      this.aerospikeClient.put(null, key, bins);
    }

    when(this.sinkTaskContext.assignment()).thenReturn(expectedOffsets.keySet());
    this.task.start(this.settings);
    verify(this.sinkTaskContext, times(1)).offset(expectedOffsets);
  }


  @Test
  public void delete() {
    final String topic = "delete";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    final long count = 100;

    //Create the keys to remove.
    for (long i = 0; i < count; i++) {
      final Key key = new Key(NAMESPACE, topic, i);
      this.aerospikeClient.put(
          null,
          key,
          new Bin("id", i)
      );

      this.recordHelper.delete(
          record -> assertFalse(
              this.aerospikeClient.exists(null, key),
              String.format("Key should be removed. Key='%s'", key)
          ),
          topic,
          Schema.INT64_SCHEMA,
          i
      );
    }

    List<SinkRecord> records = this.recordHelper.records();
    this.task.put(ImmutableList.of());
    this.task.put(records);

    verifyOffsets();

  }

  @Test
  public void put() {
    final String topic = "put";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(this.settings);

    final long count = 100;

    for (long i = 0; i < count; i++) {
      final long key = i;
      Struct struct = struct("value",
          "id", Type.INT64, true, key,
          "firstName", Type.STRING, true, "test",
          "lastName", Type.STRING, true, "user"
      );

      this.recordHelper.write(
          record -> {
            Key aerospikeKey = new Key(NAMESPACE, topic, key);
            assertTrue(
                this.aerospikeClient.exists(null, aerospikeKey),
                String.format("Expected key not found. %s", aerospikeKey)
            );
            Record aerospikeRecord = this.aerospikeClient.get(null, aerospikeKey);
            assertRecordValue(aerospikeKey, aerospikeRecord, "id", key);
            assertRecordValue(aerospikeKey, aerospikeRecord, "firstName", "test");
            assertRecordValue(aerospikeKey, aerospikeRecord, "lastName", "user");
          },
          topic,
          Schema.INT64_SCHEMA,
          key,
          struct.schema(),
          struct
      );
    }

    List<SinkRecord> records = this.recordHelper.records();
    this.task.put(ImmutableList.of());
    this.task.put(records);

    verifyOffsets();
  }

  void verifyOffsets() {
    this.recordHelper.verify();

    Map<TopicPartition, OffsetAndMetadata> offsets = this.recordHelper.offsets();
    this.task.flush(offsets);
    this.recordHelper.verifyOffsets(entry -> {
      final Key key = task.topicPartitionKey(entry.getKey());
      assertTrue(
          this.aerospikeClient.exists(null, key),
          String.format("Key for offsets %s should exist. Key %s", entry.getKey(), key)
      );
      Record record = this.aerospikeClient.get(null, key);
      assertRecordValue(key, record, "topic", entry.getKey().topic());
      assertRecordValue(key, record, "partition", (long) entry.getKey().partition());
      assertRecordValue(key, record, "offset", entry.getValue().offset());
    });

  }

  void assertRecordValue(Key key, Record record, String field, Object expected) {
    assertNotNull(
        record,
        String.format(
            "Record for %s should not be null.",
            key
        )
    );
    Object actual = record.getValue(field);
    assertEquals(
        expected,
        actual,
        String.format(
            "Value for field(%s) does not match. key = %s",
            field,
            key
        )
    );
  }

  @Test
  public void version() {
    assertNotNull(this.task.version());
  }

  @AfterEach
  public void stop() {
    this.task.stop();
  }

}
