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
import com.github.jcustenborder.kafka.connect.utils.data.AbstractConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

class AerospikeValueConverter extends AbstractConverter<List<Bin>> {
  @Override
  protected List<Bin> newValue() {
    return new ArrayList<>();
  }

  @Override
  protected void setStringField(List<Bin> bins, String fieldName, String value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setFloat32Field(List<Bin> bins, String fieldName, Float value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setFloat64Field(List<Bin> bins, String fieldName, Double value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setTimestampField(List<Bin> bins, String fieldName, Date value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setDateField(List<Bin> bins, String fieldName, Date value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setTimeField(List<Bin> bins, String fieldName, Date value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setInt8Field(List<Bin> bins, String fieldName, Byte value) {
    bins.add(
        new Bin(fieldName, value.longValue())
    );
  }

  @Override
  protected void setInt16Field(List<Bin> bins, String fieldName, Short value) {
    bins.add(
        new Bin(fieldName, value.longValue())
    );
  }

  @Override
  protected void setInt32Field(List<Bin> bins, String fieldName, Integer value) {
    bins.add(
        new Bin(fieldName, value.longValue())
    );
  }

  @Override
  protected void setInt64Field(List<Bin> bins, String fieldName, Long value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setBytesField(List<Bin> bins, String fieldName, byte[] value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setDecimalField(List<Bin> bins, String fieldName, BigDecimal value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setBooleanField(List<Bin> bins, String fieldName, Boolean value) {
    bins.add(
        new Bin(fieldName, value)
    );
  }

  @Override
  protected void setStructField(List<Bin> bins, String fieldName, Struct value) {

  }

  @Override
  protected void setArray(List<Bin> bins, String fieldName, Schema schema, List value) {

  }

  @Override
  protected void setMap(List<Bin> bins, String fieldName, Schema schema, Map value) {

  }

  @Override
  protected void setNullField(List<Bin> bins, String value) {
    bins.add(
        Bin.asNull(value)
    );
  }
}
