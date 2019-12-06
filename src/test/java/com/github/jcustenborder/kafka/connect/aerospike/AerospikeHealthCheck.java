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
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeHealthCheck implements ClusterHealthCheck {
  private static final Logger log = LoggerFactory.getLogger(AerospikeHealthCheck.class);

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) throws InterruptedException {
    Container container = cluster.container("aerospike");
    DockerPort dockerPort = container.port(3000);

    return SuccessOrFailure.onResultOf(() -> {
      log.info("Connecting {}:{}", dockerPort.getIp(), dockerPort.getExternalPort());
      try (AerospikeClient client = new AerospikeClient(dockerPort.getIp(), dockerPort.getExternalPort())) {
        return client.isConnected();
      }
    });

  }
}
