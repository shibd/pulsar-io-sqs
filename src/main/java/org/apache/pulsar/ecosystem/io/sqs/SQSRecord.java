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

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import software.amazon.awssdk.utils.StringUtils;


/**
 * A record wrapping an sqs message.
 */
public class SQSRecord implements Record<String> {
    private final String destination;
    private final com.amazonaws.services.sqs.model.Message msg;
    private final SQSSource source;

    public SQSRecord(String destination, com.amazonaws.services.sqs.model.Message msg, SQSSource source) {
        this.destination = destination;
        this.msg = msg;
        this.source = source;
    }

    @Override
    public Optional<String> getKey() {
        if (msg.getMessageAttributes().containsKey(SQSUtils.PULSAR_MESSAGE_KEY)) {
            return Optional.of(msg.getMessageAttributes().get(SQSUtils.PULSAR_MESSAGE_KEY).getStringValue());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Schema<String> getSchema() {
        return Schema.STRING;
    }

    @Override
    public String getValue() {
        return msg.getBody();
    }

    @Override
    public Optional<Long> getEventTime() {
        if (msg.getAttributes().containsKey(MessageSystemAttributeName.SentTimestamp.toString())) {
            return Optional.of(
                    Long.parseLong(msg.getAttributes().get(MessageSystemAttributeName.SentTimestamp.toString())));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, MessageAttributeValue> attribute: msg.getMessageAttributes().entrySet()) {
            if (!attribute.getKey().equals(SQSUtils.PULSAR_MESSAGE_KEY)) {
                properties.put(attribute.getKey(), attribute.getValue().getStringValue());
            }
        }
        return properties;
    }

    @Override
    public void ack() {
        source.ack(msg.getReceiptHandle());
    }

    @Override
    public void fail() {
        source.fail(msg.getReceiptHandle());
    }

    @Override
    public Optional<String> getDestinationTopic() {
        if (msg.getMessageAttributes().containsKey(SQSUtils.PULSAR_TOPIC_ATTRIBUTE)) {
            String topicName = msg.getMessageAttributes().get(SQSUtils.PULSAR_TOPIC_ATTRIBUTE).getStringValue();
            if (StringUtils.isNotBlank(topicName)) {
                throw new IllegalArgumentException("topicName cannot be blank");
            }
            return Optional.of(StringUtils.trim(topicName));
        } else {
            return Optional.of(destination);
        }
    }
}
