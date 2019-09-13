package com.github.jcustenborder.kafka.connect.aerospike;

import com.aerospike.client.AerospikeClient;
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class AerospikeHealthCheck implements ClusterHealthCheck {
  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) throws InterruptedException {
    Container container = cluster.container("aerospike");
    DockerPort dockerPort = container.port(3000);
    try (AerospikeClient client = new AerospikeClient(dockerPort.getIp(), dockerPort.getExternalPort())) {
      return SuccessOrFailure.fromBoolean(client.isConnected(), "Client not connected");
    }
  }
}
