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

import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class MySinkRecordHelper extends SinkRecordHelper {
  private static final Logger log = LoggerFactory.getLogger(MySinkRecordHelper.class);
  Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
  private List<State> states = new ArrayList<>();

  public Map<TopicPartition, OffsetAndMetadata> offsets() {
    return offsets;
  }

  public void verifyOffsets(Consumer<Map.Entry<TopicPartition, OffsetAndMetadata>> entryConsumer) {
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : this.offsets.entrySet()) {
      log.info(
          "verifyOffsets() - Calling verification handler for {}:{}",
          entry.getKey(),
          entry.getValue()
      );
      entryConsumer.accept(entry);
    }
  }

  public void verify() {
    for (State state : this.states) {
      log.info(
          "verify() - Calling verification handler for {}:{}:{}",
          state.record.topic(),
          state.record.kafkaPartition(),
          state.record.kafkaOffset()
      );
      state.verification.accept(state.record);
    }
  }

  public List<SinkRecord> records() {
    return this.states.stream()
        .map(s -> s.record)
        .collect(Collectors.toList());
  }

  public void clear() {
    this.states.clear();
    this.offsets.clear();
  }

  long nextOffset(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    OffsetAndMetadata offsetAndMetadata = this.offsets.compute(topicPartition, new BiFunction<TopicPartition, OffsetAndMetadata, OffsetAndMetadata>() {
      @Override
      public OffsetAndMetadata apply(TopicPartition topicPartition, OffsetAndMetadata current) {
        OffsetAndMetadata result;
        if (null == current) {
          result = new OffsetAndMetadata(0L, null);
        } else {
          result = new OffsetAndMetadata(current.offset() + 1L, null);
        }
        return result;
      }
    });
    return offsetAndMetadata.offset();
  }

  public SinkRecord write(Consumer<SinkRecord> validate, String topic, Schema keySchema, Object key, Schema valueSchema, Object value) {
    final int partition = 0;
    final long offset = nextOffset(topic, partition);
    SinkRecord result = new SinkRecord(
        topic,
        partition,
        keySchema,
        key,
        valueSchema,
        value,
        offset
    );
    State state = State.of(validate, result);
    this.states.add(state);
    return result;
  }

  public SinkRecord delete(Consumer<SinkRecord> validate, String topic, Schema keySchema, Object key) {
    final int partition = 0;
    final long offset = nextOffset(topic, partition);
    SinkRecord result = new SinkRecord(
        topic,
        partition,
        keySchema,
        key,
        null,
        null,
        offset
    );
    State state = State.of(validate, result);
    this.states.add(state);
    return result;
  }

  static class State {
    final Consumer<SinkRecord> verification;
    final SinkRecord record;

    State(Consumer<SinkRecord> verification, SinkRecord record) {
      this.verification = verification;
      this.record = record;
    }

    public static State of(Consumer<SinkRecord> verification, SinkRecord record) {
      return new State(verification, record);
    }
  }

}
