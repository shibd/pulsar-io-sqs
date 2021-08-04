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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A source connector for AWS SQS.
 */
@Slf4j
public class SQSSink extends SQSAbstractConnector implements Sink<GenericRecord> {
    private SinkContext sinkContext;

    private static final String METRICS_TOTAL_SUCCESS = "_sqs_sink_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_sqs_sink_total_failure_";
    private static final String PULSAR_MESSAGE_KEY = "pulsar.key";

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        this.sinkContext = sinkContext;
        setConfig(SQSConnectorConfig.load(map));
        prepareSqsClient();
    }

    @Override
    public void write(Record<GenericRecord> record) {
        SendMessageRequest request = generateSendMessageRequest(record);
        if (request == null) {
            record.ack();
            return;
        }

        getClient().sendMessageAsync(request, new AsyncHandler<SendMessageRequest, SendMessageResult>() {
            @Override
            public void onError(Exception e) {
                log.error("failed sending message to AWS SQS.", e);
                record.fail();
                if (sinkContext != null) {
                    sinkContext.recordMetric(METRICS_TOTAL_FAILURE, 1);
                }
            }

            @Override
            public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult) {
                record.ack();
                if (sinkContext != null) {
                    sinkContext.recordMetric(METRICS_TOTAL_SUCCESS, 1);
                }
            }
        });
    }

    private SendMessageRequest generateSendMessageRequest(Record<GenericRecord> record) {
        String msgBody = generateMessageBody(record);
        if (null == msgBody) {
            return null;
        }

        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(getQueueUrl())
                .withMessageBody(msgBody);

        Map<String, MessageAttributeValue> attributes = generateMessageAttributes(record);
        if (!attributes.isEmpty()) {
            request.withMessageAttributes(attributes);
        }

        return request;
    }

    private String generateMessageBody(Record<GenericRecord> record) {
        if (record.getSchema() == null) {
            return new String(record.getMessage().get().getData(), UTF_8);
        } else {
            Object nativeObject = record.getValue().getNativeObject();
            if (null == nativeObject) {
                return null;
            } else {
                return nativeObject.toString();
            }
        }
    }

    private Map<String, MessageAttributeValue> generateMessageAttributes(Record<GenericRecord> record) {
        Map<String, MessageAttributeValue> attributeMap = new HashMap<>();

        if (record.getKey().isPresent()) {
            attributeMap.put(PULSAR_MESSAGE_KEY, new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(record.getKey().get()));
        }

        for (Map.Entry<String, String> propertyEntry: record.getProperties().entrySet()) {
            if (propertyEntry.getKey() == PULSAR_MESSAGE_KEY) {
                log.error("attribute name " + PULSAR_MESSAGE_KEY + "is reserved.");
                throw new IllegalArgumentException(PULSAR_MESSAGE_KEY + " is a reserved attributed key.");
            }

           attributeMap.put(propertyEntry.getKey(), new MessageAttributeValue()
                   .withDataType("String")
                   .withStringValue(propertyEntry.getValue()));
        }

        return attributeMap;
    }

    @Override
    public void close() {
        if (getClient() != null) {
            getClient().shutdown();
        }
    }
}
