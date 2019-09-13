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
