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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.sqs.convert.DefaultRecordConverter;
import org.apache.pulsar.ecosystem.io.sqs.convert.MetaDataConverter;
import org.apache.pulsar.ecosystem.io.sqs.convert.RecordConvertException;
import org.apache.pulsar.ecosystem.io.sqs.convert.RecordConverter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A sink connector for AWS SQS.
 */
@Slf4j
public class SQSSink extends SQSAbstractConnector implements Sink<GenericRecord> {
    private SinkContext sinkContext;
    private RecordConverter recordConverter;
    private MetaDataConverter metaDataConverter;

    private static final String METRICS_TOTAL_SUCCESS = "_sqs_sink_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_sqs_sink_total_failure_";

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        SQSConnectorConfig config = SQSConnectorConfig.load(map, sinkContext);
        this.sinkContext = sinkContext;
        this.recordConverter = new DefaultRecordConverter();
        this.metaDataConverter = new MetaDataConverter(config.getMetaDataFields());
        setConfig(config);
        prepareSqsClient();
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        SendMessageRequest request = generateSendMessageRequest(record);
        if (request == null) {
            record.ack();
            return;
        }

        getClient().sendMessageAsync(request, new AsyncHandler<>() {
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

    private SendMessageRequest generateSendMessageRequest(Record<GenericRecord> record) throws RecordConvertException {
        String msgBody = recordConverter.convertToJson(record);
        if (null == msgBody) {
            return null;
        }

        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(getQueueUrl())
                .withMessageBody(msgBody);

        Map<String, MessageAttributeValue> attributes = metaDataConverter.convert(record);
        if (!attributes.isEmpty()) {
            request.withMessageAttributes(attributes);
        }

        return request;
    }

    @Override
    public void close() {
        if (getClient() != null) {
            getClient().shutdown();
        }
    }
}
