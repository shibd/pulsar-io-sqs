/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.sqs;

import lombok.extern.slf4j.Slf4j;

/**
 * The sqs consumer thread class for {@link SQSSource}.
 */
@Slf4j
public class SQSConsumerThread extends Thread {

    private final SQSSource source;

    public SQSConsumerThread(SQSSource source) {
        this.source = source;
    }

    public void run() {
        while (true) {
            try {
                source.receive().forEachOrdered(source::enqueue);
            } catch (Exception ex) {
                log.error("receive message from sqs error", ex);
            }
        }
    }
}
