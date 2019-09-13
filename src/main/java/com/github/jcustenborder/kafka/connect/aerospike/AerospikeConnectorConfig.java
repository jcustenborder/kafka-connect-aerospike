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

import com.aerospike.client.Host;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.google.common.base.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;
import java.util.concurrent.TimeUnit;

class AerospikeConnectorConfig extends AbstractConfig {
  public static final String NAMESPACE_CONFIG = "namespace";
  public static final String CONN_AUTH_MODE_CONFIG = "connection.auth.mode";


  public static final String BATCH_ALLOW_INLINE_CONFIG = "batch.inline.enabled";
  public static final String BATCH_CONCURRENT_THREADS_MAX_CONFIG = "batch.concurrent.threads.max";
  public static final String CONNECT_OFFSET_SET_CONFIG = "connect.offset.set.name";
  public static final String CONNECT_OFFSET_SET_DOC = "The set name in the namespace that is used " +
      "to store the offsets that have been successfully written to Aerospike.";
  protected static final String GROUP_CONNECTION = "Connection";
  protected static final String GROUP_BATCH = "Batching";
  protected static final String GROUP_NAMESPACE = "Namespace";
  static final String NAMESPACE_DOC = "";
  static final String CONN_AUTH_MODE_DOC = "Authentication mode used when user/password is defined. " +
      "`EXTERNAL` - Use external authentication (like LDAP). `EXTERNAL_INSECURE` - Use external " +
      "authentication (like LDAP). `INTERNAL` - Use internal authentication only.";
  static final String BATCH_ALLOW_INLINE_DOC = "Allow batch to be processed immediately in the " +
      "server's receiving thread when the server deems it to be appropriate.";
  static final String BATCH_CONCURRENT_THREADS_MAX_DOC = "Maximum number of concurrent synchronous " +
      "batch request threads to server nodes at any point in time.";

  public static final String HOSTS_CONFIG = "hosts";
  public static final String HOSTS_DOC = "Hostnames are parsed from the standard Aerospike format. Ex `hostname1[:tlsname1][:port1],...`";
  public static final String HOSTS_DEFAULT_PORT_CONFIG = "hosts.default.port";
  public static final String HOSTS_DEFAULT_PORT_DOC = "The default port to use to connect to Aerospike.";


  public final String namespace;
  public final String offsetSetName;
  public final String hosts;
  public final int hostDefaultPort;
  public final AuthMode authMode;
  public final String username;
  public final String password;
  public final String connClusterName;
  public final int connMaxConnsPerNode;
  public final int connMaxSocketIdle;
  public final boolean connRackAwareEnabled;
  public final int connRackId;

  public static final String DEFAULT_CONNECT_OFFSET_SET = "_connect_offsets";

  public AerospikeConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.namespace = getString(NAMESPACE_CONFIG);
    this.offsetSetName = getString(CONNECT_OFFSET_SET_CONFIG);
    this.hosts = getString(HOSTS_CONFIG);
    this.hostDefaultPort = getInt(HOSTS_DEFAULT_PORT_CONFIG);
    this.authMode = ConfigUtils.getEnum(AuthMode.class, this, CONN_AUTH_MODE_CONFIG);
    String username = getString(CONN_USERNAME_CONFIG);
    this.username = Strings.isNullOrEmpty(username) ? null : username;
    String password = getString(CONN_PASSWORD_CONFIG);
    this.password = Strings.isNullOrEmpty(username) ? null : password;
    String connClusterName = getString(CONN_CLUSTER_NAME_CONFIG);
    this.connClusterName = Strings.isNullOrEmpty(connClusterName) ? null : connClusterName;
    this.connMaxConnsPerNode = getInt(CONN_CONNECTION_MAX_PER_NODE_CONFIG);
    this.connMaxSocketIdle = (int) TimeUnit.MILLISECONDS.toSeconds(
        (long) getInt(CONN_CONNECTION_MAX_SOCKET_IDLE_CONFIG)
    );
    this.connRackAwareEnabled = getBoolean(CONN_RACK_ENABLED_CONFIG);
    this.connRackId = getInt(CONN_RACK_ID_CONFIG);
  }

  public static final String CONN_USERNAME_CONFIG = "connection.username";
  public static final String CONN_USERNAME_DOC = "User authentication to cluster. Leave null for clusters running without restricted access.";
  public static final String CONN_PASSWORD_CONFIG = "connection.password";
  public static final String CONN_PASSWORD_DOC = "Password authentication to cluster. The password will be stored by the client and sent to server in hashed format. Leave null for clusters running without restricted access.";
  public static final String CONN_CLUSTER_NAME_CONFIG = "connection.cluster.name";
  public static final String CONN_CLUSTER_NAME_DOC = "Expected cluster name. If not null, server nodes must return this cluster name in order to join the client's view of the cluster. Should only be set when connecting to servers that support `the cluster-name` info command.";
  public static final String CONN_CONNECTION_POOLS_PER_NODE_CONFIG = "connection.pools.per.node";
  public static final String CONN_CONNECTION_POOLS_PER_NODE_DOC = "Number of synchronous connection pools used for each node.";

  public static final String CONN_CONNECTION_MAX_PER_NODE_CONFIG = "connection.per.node.max";
  public static final String CONN_CONNECTION_MAX_PER_NODE_DOC = "Maximum number of connections allowed per server node.";

  public static final String CONN_CONNECTION_MAX_SOCKET_IDLE_CONFIG = "connection.socket.idle.timeout.ms";
  public static final String CONN_CONNECTION_MAX_SOCKET_IDLE_DOC = "Maximum socket idle in milliseconds.";

  public static final String CONN_CONNECTION_LOGIN_TIMEOUT_CONFIG = "connection.login.timeout.ms";
  public static final String CONN_CONNECTION_LOGIN_TIMEOUT_DOC = "Login timeout in milliseconds.";

  public static final String CONN_RACK_ID_CONFIG = "connection.rack.aware.id";
  public static final String CONN_RACK_ID_DOC = "Rack where this client instance resides.";

  public static final String CONN_RACK_ENABLED_CONFIG = "connection.rack.aware.enabled";
  public static final String CONN_RACK_ENABLED_DOC = "Flag to determine if the client should track server rack data.";

  public static ConfigDef config() {
    //Create an instance of a client policy to grab the defaults from.
    final ClientPolicy defaultClientPolicy = new ClientPolicy();

    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(NAMESPACE_CONFIG, Type.STRING)
                .documentation(NAMESPACE_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_NAMESPACE)
                .build()
        ).define(
            ConfigKeyBuilder.of(CONNECT_OFFSET_SET_CONFIG, Type.STRING)
                .documentation(CONNECT_OFFSET_SET_DOC)
                .importance(Importance.LOW)
                .defaultValue(DEFAULT_CONNECT_OFFSET_SET)
                .group(GROUP_NAMESPACE)
                .build()
        )
        // Connection
        .define(
            ConfigKeyBuilder.of(HOSTS_CONFIG, Type.STRING)
                .documentation(HOSTS_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_CONNECTION)
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_USERNAME_CONFIG, Type.STRING)
                .documentation(CONN_USERNAME_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_CONNECTION)
                .defaultValue("")
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_PASSWORD_CONFIG, Type.STRING)
                .documentation(CONN_PASSWORD_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_CONNECTION)
                .defaultValue("")
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_CLUSTER_NAME_CONFIG, Type.STRING)
                .documentation(CONN_CLUSTER_NAME_DOC)
                .importance(Importance.MEDIUM)
                .group(GROUP_CONNECTION)
                .defaultValue("")
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_CONNECTION_POOLS_PER_NODE_CONFIG, Type.INT)
                .documentation(CONN_CONNECTION_POOLS_PER_NODE_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(defaultClientPolicy.connPoolsPerNode)
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_CONNECTION_MAX_PER_NODE_CONFIG, Type.INT)
                .documentation(CONN_CONNECTION_MAX_PER_NODE_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(defaultClientPolicy.maxConnsPerNode)
                .validator(ConfigDef.Range.atLeast(1))
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_CONNECTION_MAX_SOCKET_IDLE_CONFIG, Type.INT)
                .documentation(CONN_CONNECTION_MAX_SOCKET_IDLE_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(
                    (int) TimeUnit.SECONDS.toMillis(defaultClientPolicy.maxConnsPerNode)
                )
                .validator(ConfigDef.Range.atLeast(1000))
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_CONNECTION_LOGIN_TIMEOUT_CONFIG, Type.INT)
                .documentation(CONN_CONNECTION_LOGIN_TIMEOUT_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(defaultClientPolicy.loginTimeout)
                .validator(ConfigDef.Range.atLeast(1000))
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_RACK_ENABLED_CONFIG, Type.BOOLEAN)
                .documentation(CONN_RACK_ENABLED_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(defaultClientPolicy.rackAware)
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_RACK_ID_CONFIG, Type.INT)
                .documentation(CONN_RACK_ID_DOC)
                .importance(Importance.LOW)
                .group(GROUP_CONNECTION)
                .defaultValue(defaultClientPolicy.rackId)
                .build()
        )

        .define(
            ConfigKeyBuilder.of(HOSTS_DEFAULT_PORT_CONFIG, Type.INT)
                .documentation(HOSTS_DEFAULT_PORT_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_CONNECTION)
                .defaultValue(3000)
                .validator(Validators.validPort())
                .build()
        ).define(
            ConfigKeyBuilder.of(CONN_AUTH_MODE_CONFIG, Type.STRING)
                .documentation(CONN_AUTH_MODE_DOC)
                .importance(Importance.HIGH)
                .group(GROUP_CONNECTION)
                .validator(Validators.validEnum(AuthMode.class))
                .recommender(Recommenders.enumValues(AuthMode.class))
                .defaultValue(defaultClientPolicy.authMode.toString())
                .build()
        )
        // Batch
        .define(
            ConfigKeyBuilder.of(BATCH_ALLOW_INLINE_CONFIG, Type.BOOLEAN)
                .documentation(BATCH_ALLOW_INLINE_DOC)
                .importance(Importance.LOW)
                .group(GROUP_BATCH)
                .defaultValue(defaultClientPolicy.batchPolicyDefault.allowInline)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(BATCH_CONCURRENT_THREADS_MAX_CONFIG, Type.INT)
                .documentation(BATCH_CONCURRENT_THREADS_MAX_DOC)
                .importance(Importance.LOW)
                .group(GROUP_BATCH)
                .defaultValue(defaultClientPolicy.batchPolicyDefault.maxConcurrentThreads)
                .build()
        );
  }

  public Host[] hosts() {
    return Host.parseHosts(this.hosts, this.hostDefaultPort);
  }

  public ClientPolicy clientPolicy() {
    ClientPolicy result = new ClientPolicy();
    result.failIfNotConnected = true;
    result.authMode = this.authMode;
    result.maxConnsPerNode = this.connMaxConnsPerNode;
    result.maxSocketIdle = this.connMaxSocketIdle;
    result.rackAware = this.connRackAwareEnabled;
    result.rackId = this.connRackId;

    if (!Strings.isNullOrEmpty(this.username)) {
      result.user = this.username;
    }
    if (!Strings.isNullOrEmpty(this.password)) {
      result.password = this.password;
    }
    if (!Strings.isNullOrEmpty(this.connClusterName)) {
      result.clusterName = this.connClusterName;
    }
    return result;
  }
}
