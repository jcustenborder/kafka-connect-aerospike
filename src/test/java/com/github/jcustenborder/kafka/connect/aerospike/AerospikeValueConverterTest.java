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
import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AerospikeValueConverterTest {
  AerospikeValueConverter converter;

  @BeforeEach
  public void before() {
    this.converter = new AerospikeValueConverter();
  }

  @Test
  public void struct() {
    Schema schema = SchemaBuilder.struct()
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("firstName", "test")
        .put("lastName", "user");

    List<Bin> bins = this.converter.convert(struct);
    assertNotNull(bins);
    assertEquals(
        ImmutableList.of(
            new Bin("firstName", "test"),
            new Bin("lastName", "user")
        ),
        bins
    );


  }


}
