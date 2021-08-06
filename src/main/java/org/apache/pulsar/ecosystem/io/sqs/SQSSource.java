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
/*
 * Classes for implementing a pulsar IO connector for AWS SQS.
 */
package org.apache.pulsar.ecosystem.io.sqs;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A source connector for AWS SQS.
 */
@Slf4j
public class SQSSource extends SQSAbstractConnector implements Source<String> {

    private static final int DEFAULT_QUEUE_LENGTH = 1000;

    private static final String METRICS_TOTAL_SUCCESS = "_sqs_source_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_sqs_source_total_failure_";
    private String destinationTopic;
    private SourceContext sourceContext;
    private ExecutorService executor;
    private LinkedBlockingQueue<Record<String>> queue;

    @Override
    public void open(Map<String, Object> map, SourceContext sourceContext) throws Exception {
        this.sourceContext = sourceContext;
        setConfig(SQSConnectorConfig.load(map));
        prepareSqsClient();

        destinationTopic = sourceContext.getOutputTopic();
        queue = new LinkedBlockingQueue<>(this.getQueueLength());

        executor = Executors.newFixedThreadPool(1);
        executor.execute(new SQSConsumerThread(this));
    }

    public void fail(String messageHandle) {
        final ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest()
                .withQueueUrl(getQueueUrl())
                .withReceiptHandle(messageHandle)
                .withVisibilityTimeout(SQSUtils.MAX_WAIT_TIME);

        getClient().changeMessageVisibilityAsync(request,
                new AsyncHandler<ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult>() {
                    @Override
                    public void onError(Exception e) {
                        fail(messageHandle); // retry
                    }

                    @Override
                    public void onSuccess(ChangeMessageVisibilityRequest request,
                                          ChangeMessageVisibilityResult changeMessageVisibilityResult) {
                        if (sourceContext != null) {
                            sourceContext.recordMetric(METRICS_TOTAL_FAILURE, 1);
                        }
                    }
                }
        );
    }

    public void ack(String messageHandle) {
        final DeleteMessageRequest request = new DeleteMessageRequest()
                .withQueueUrl(getQueueUrl())
                .withReceiptHandle(messageHandle);

        getClient().deleteMessageAsync(request, new AsyncHandler<DeleteMessageRequest, DeleteMessageResult>() {
            @Override
            public void onError(Exception e) {
                ack(messageHandle); // retry
            }

            @Override
            public void onSuccess(DeleteMessageRequest request, DeleteMessageResult deleteMessageResult) {
                // do nothing
                if (sourceContext != null) {
                    sourceContext.recordMetric(METRICS_TOTAL_SUCCESS, 1);
                }
            }
        });
    }

    @Override
    public Record<String> read() throws InterruptedException {
            return this.queue.take();
    }

    public void enqueue(Message msg) {
        try {
            this.queue.put(new SQSRecord(destinationTopic, msg, this));
        } catch (InterruptedException ex) {
            log.error("sqs message processing interrupted", ex);
            fail(msg.getReceiptHandle());
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                // wait a while for tasks to respond to being cancelled
                executor.awaitTermination(3000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (getClient() != null) {
            getClient().shutdown();
        }

        log.info("SQSSource closed.");
    }

    public int getQueueLength() {
        return DEFAULT_QUEUE_LENGTH;
    }

}

