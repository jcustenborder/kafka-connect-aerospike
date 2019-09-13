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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AerospikeClientFactoryImpl implements AerospikeClientFactory {
  private static final Logger log = LoggerFactory.getLogger(AerospikeClientFactoryImpl.class);

  static {
    Log.setCallback(AerospikeLogCallback.INSTANCE);
    Log.setLevel(Log.Level.DEBUG);
  }

  @Override
  public IAerospikeClient create(AerospikeConnectorConfig config) {
    ClientPolicy clientPolicy = config.clientPolicy();
    Host[] hosts = config.hosts();
    log.info("create() - Connecting to Aerospike.");

    return new AerospikeClient(clientPolicy, hosts);
  }
}
