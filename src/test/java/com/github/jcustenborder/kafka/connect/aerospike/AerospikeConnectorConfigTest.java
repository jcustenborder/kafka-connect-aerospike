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

import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class AerospikeConnectorConfigTest {
  Map<String, String> settings;

  @BeforeEach
  public void before() {
    this.settings = new LinkedHashMap<>();
    this.settings.put(AerospikeConnectorConfig.NAMESPACE_CONFIG, "test");
    this.settings.put(AerospikeConnectorConfig.HOSTS_CONFIG, "localhost");
  }

  <T> DynamicTest test(String key, T expected, Function<ClientPolicy, T> verify) {
    return dynamicTest(key, () -> {
      this.settings.put(key, expected.toString());
      AerospikeConnectorConfig config = new AerospikeConnectorConfig(this.settings);
      ClientPolicy clientPolicy = config.clientPolicy();
      T actual = verify.apply(clientPolicy);
      assertEquals(expected, actual, key + " does not match expected.");
    });
  }

  @TestFactory
  public Stream<DynamicTest> clientPolicy() {
    return Stream.of(
        test(AerospikeConnectorConfig.WRITE_POLICY_SEND_KEY_CONF, true, clientPolicy -> clientPolicy.writePolicyDefault.sendKey),
        test(AerospikeConnectorConfig.WRITE_POLICY_DURABLE_DELETE_CONF, true, clientPolicy -> clientPolicy.writePolicyDefault.durableDelete),
        test(AerospikeConnectorConfig.CONN_AUTH_MODE_CONFIG, AuthMode.EXTERNAL, clientPolicy -> clientPolicy.authMode),
        test(AerospikeConnectorConfig.CONN_RACK_ENABLED_CONFIG, true, clientPolicy -> clientPolicy.rackAware),
        test(AerospikeConnectorConfig.CONN_RACK_ID_CONFIG, 4, clientPolicy -> clientPolicy.rackId),
        test(AerospikeConnectorConfig.CONN_PASSWORD_CONFIG, "password1234", clientPolicy -> clientPolicy.password),
        test(AerospikeConnectorConfig.CONN_USERNAME_CONFIG, "user123", clientPolicy -> clientPolicy.user),
        test(AerospikeConnectorConfig.CONN_CLUSTER_NAME_CONFIG, "cluster1", clientPolicy -> clientPolicy.clusterName),
        test(AerospikeConnectorConfig.CONN_CONNECTION_LOGIN_TIMEOUT_CONFIG, 12345, clientPolicy -> clientPolicy.loginTimeout)
    );
  }


}
