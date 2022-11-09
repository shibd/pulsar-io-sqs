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

import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;

/**
 * Abstract Class for pulsar connector to AWS SQS.
 */
@Slf4j
public abstract class SQSAbstractConnector extends AbstractAwsConnector {
    // There will be a bottleneck when use single AmazonSQSBufferedAsyncClient to delete.
    // If consumer threads exceed the threshold n times, n clients will be created for asynchronous delete tasks.
    public static final int SQS_CLIENT_THRESHOLD = 10;
    @Getter
    @Setter
    private SQSConnectorConfig config;

    @Getter
    private AmazonSQSBufferedAsyncClient client;

    @Getter
    private ArrayList<AmazonSQSBufferedAsyncClient> clientsForDelete;

    private final AtomicLong index = new AtomicLong(0);

    private int deleteClientCount = 0;

    @Getter
    private String queueUrl;

    public void prepareSqsClient() throws Exception {
        if (config == null) {
            throw new IllegalStateException("Configuration not set");
        }
        if (client != null) {
            throw new IllegalStateException("Connector is already open");
        }

        AwsCredentialProviderPlugin credentialsProvider = createCredentialProvider(
                config.getAwsCredentialPluginName(),
                config.getAwsCredentialPluginParam());

        client = config.buildAmazonSQSClient(credentialsProvider);

        queueUrl = SQSUtils.ensureQueueExists(client, config.getQueueName());

        if (config.getNumberOfConsumers() > SQS_CLIENT_THRESHOLD) {
            deleteClientCount = config.getNumberOfConsumers() / SQS_CLIENT_THRESHOLD + 1;
            clientsForDelete = new ArrayList<>(deleteClientCount);

            IntStream.range(0, deleteClientCount)
                    .forEach((i) -> clientsForDelete.add(config.buildAmazonSQSClient(credentialsProvider)));
        }
    }

    public AmazonSQSBufferedAsyncClient getDeleteClient() {
        if (deleteClientCount > 0) {
            return clientsForDelete.get((int) (index.getAndIncrement() % deleteClientCount));
        }
        return client;
    }
}
