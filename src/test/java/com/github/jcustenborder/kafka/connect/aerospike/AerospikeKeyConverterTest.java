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
import com.google.common.base.MoreObjects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class AerospikeKeyConverterTest {

  static final String NAMESPACE = "testing";
  static final String TOPIC = "testing-topic";

  AerospikeKeyConverter converter;

  static TestCase test(Object value, Value keyValue) {
    Key expected = new Key(NAMESPACE, TOPIC, keyValue);
    return new TestCase(new SchemaAndValue(null, value), expected);
  }

  static TestCase test(Schema schema, Object value, Value keyValue) {
    Key expected = new Key(NAMESPACE, TOPIC, keyValue);
    return new TestCase(new SchemaAndValue(schema, value), expected);
  }

  @BeforeEach
  public void before() {
    this.converter = new AerospikeKeyConverter(NAMESPACE);
  }

  SinkRecord record(SchemaAndValue schemaAndValue) {
    return new SinkRecord(
        TOPIC,
        0,
        schemaAndValue.schema(),
        schemaAndValue.value(),
        null,
        null,
        1234L
    );
  }

  @Test
  public void nullKey() {
    SinkRecord record = record(SchemaAndValue.NULL);
    DataException exception = assertThrows(DataException.class, () -> {
      this.converter.convert(record);
    });
  }

  @Test
  public void structKey() {
    Schema schema = SchemaBuilder.struct()
        .field("firstName", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("firstName", "jeremy");
    SinkRecord record = record(new SchemaAndValue(schema, struct));
    DataException exception = assertThrows(DataException.class, () -> {
      this.converter.convert(record);
    });
  }

  @TestFactory
  public Stream<DynamicTest> convertSchema() {
    return Arrays.asList(
        test(Schema.STRING_SCHEMA, "foo", Value.get("foo")),
        test(Schema.INT64_SCHEMA, Long.MAX_VALUE, Value.get(Long.MAX_VALUE)),
        test(Schema.INT32_SCHEMA, Integer.MAX_VALUE, Value.get(Integer.MAX_VALUE)),
        test(Schema.INT8_SCHEMA, Byte.MAX_VALUE, Value.get(Byte.MAX_VALUE))
    ).stream()
        .map(testCase -> dynamicTest(testCase.toString(), () -> {
          SinkRecord record = record(testCase.input);
          Key actual = this.converter.convert(record);
          assertNotNull(actual);
          assertEquals(testCase.expected, actual);
        }));
  }

  @TestFactory
  public Stream<DynamicTest> convertSchemaLess() {
    return Arrays.asList(
        test("foo", Value.get("foo")),
        test(Long.MAX_VALUE, Value.get(Long.MAX_VALUE)),
        test(Integer.MAX_VALUE, Value.get(Integer.MAX_VALUE)),
        test(Byte.MAX_VALUE, Value.get(Byte.MAX_VALUE))
    ).stream()
        .map(testCase -> dynamicTest(testCase.toString(), () -> {
          SinkRecord record = record(testCase.input);
          Key actual = this.converter.convert(record);
          assertNotNull(actual);
          assertEquals(testCase.expected, actual);
        }));
  }

  static class TestCase {
    final SchemaAndValue input;
    final Key expected;

    TestCase(SchemaAndValue input, Key expected) {
      this.input = input;
      this.expected = expected;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("schema", input.schema())
          .add("value", input.value())
          .add("valueClass", input.value().getClass().getSimpleName())
          .toString();
    }
  }


}
