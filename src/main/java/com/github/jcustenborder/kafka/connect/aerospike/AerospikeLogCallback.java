package com.github.jcustenborder.kafka.connect.aerospike;

import com.aerospike.client.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AerospikeLogCallback implements Log.Callback {
  public static final Log.Callback INSTANCE = new AerospikeLogCallback();

  private static final Logger log = LoggerFactory.getLogger(AerospikeLogCallback.class);

  @Override
  public void log(Log.Level level, String s) {
    switch (level) {
      case INFO:
        log.info(s);
        break;
      case WARN:
        log.warn(s);
        break;
      case DEBUG:
        log.debug(s);
        break;
      case ERROR:
        log.error(s);
        break;
    }
  }
}
