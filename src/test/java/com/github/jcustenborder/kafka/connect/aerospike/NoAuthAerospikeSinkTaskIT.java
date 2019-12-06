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
import com.aerospike.client.IAerospikeClient;
import com.github.jcustenborder.docker.junit5.Compose;

import java.net.InetSocketAddress;

@Compose(dockerComposePath = "src/test/resources/noauth-docker-compose.yml", clusterHealthCheck = AerospikeHealthCheck.class)
public class NoAuthAerospikeSinkTaskIT extends AbstractAerospikeSinkTaskIT {
  @Override
  protected IAerospikeClient client(InetSocketAddress address) {
    return new AerospikeClient(address.getHostString(), address.getPort());
  }
}
