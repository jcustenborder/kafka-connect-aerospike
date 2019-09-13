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

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;

class AerospikeKeyConverter {
  static final Set<Class<?>> ALLOWED_CLASSES = ImmutableSet.of(
      String.class,
      Long.class,
      Integer.class,
      Byte.class
  );
  static final Set<Schema.Type> ALLOWED_TYPES = ImmutableSet.of(
      Schema.Type.INT8,
      Schema.Type.INT32,
      Schema.Type.INT64,
      Schema.Type.STRING,
      Schema.Type.BYTES
  );
  final String namespace;
  AerospikeKeyConverter(String namespace) {
    this.namespace = namespace;
  }

  static DataException recordException(SinkRecord record, Throwable throwable, String format, Object... args) {
    String message = String.format(
        "Exception thrown for record: (%s:%s:%s): ",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    ) + String.format(format, args);

    return new DataException(message, throwable);
  }

  private Key convertSchema(SinkRecord record) {
    if (!ALLOWED_TYPES.contains(record.keySchema().type())) {
      throw recordException(
          record,
          null,
          "%s is not a supported key type.record.key() must be integer, string, bytes, or double",
          record.key().getClass().getName()
      );
    }

    Value keyValue = Value.get(record.key());
    return new Key(this.namespace, record.topic(), keyValue);
  }

  private Key convertSchemaLess(SinkRecord record) {
    if (!ALLOWED_CLASSES.contains(record.key().getClass())) {
      throw recordException(
          record,
          null,
          "%s is not a supported key type.record.key() must be integer, string, bytes, or double",
          record.key().getClass().getName()
      );
    }

    Value keyValue = Value.get(record.key());
    return new Key(this.namespace, record.topic(), keyValue);
  }

  public Key convert(SinkRecord record) {
    Key result;

    if (null == record.key()) {
      throw recordException(
          record,
          null,
          "record.key() cannot be null. record.key() must be integer, string, bytes, or double"
      );
    } else if (null != record.keySchema()) {
      result = convertSchema(record);
    } else {
      result = convertSchemaLess(record);
    }

    return result;
  }

}
