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
