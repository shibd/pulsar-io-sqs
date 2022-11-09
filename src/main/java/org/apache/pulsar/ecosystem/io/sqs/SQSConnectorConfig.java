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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * The configuration class for {@link SQSSink} and {@link SQSSource}.
 */
@Data
@Slf4j
public class SQSConnectorConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_BATCH_SIZE_OF_ONCE_RECEIVE = 1;
    public static final int DEFAULT_NUMBER_OF_SQS_CONSUMERS = 1;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "AWS SQS end-point url. It can be found at https://docs.aws.amazon.com/general/latest/gr/rande.html"
    )
    private String awsEndpoint = "";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Appropriate aws region. E.g. us-west-1, us-west-2"
    )
    private String awsRegion;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The SQS queue name that messages should be read from or written to."
    )
    private String queueName;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin."
                    + " It is a factory class which creates an AWSCredentialsProvider that will be used by sqs client."
                    + " If it is empty then sqs client will create a default AWSCredentialsProvider which accepts json"
                    + " of credentials in `awsCredentialPluginParam`")
    private String awsCredentialPluginName = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private String awsCredentialPluginParam = "";

    @FieldDoc(
            required = false,
            defaultValue = "1",
            help = "The maximum number of messages pulled from SQS at one time for SQS source. Default=1 and the max "
                    + "value=10.")
    private int batchSizeOfOnceReceive;

    @FieldDoc(required = false,
            defaultValue = "1",
            help = "The expected numbers of consumers for SQS source. You can scale message consumers horizontally to "
                    + "achieve high throughput. Default=1 and the max value=50.")
    private int numberOfConsumers;

    public static SQSConnectorConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SQSConnectorConfig.class);
    }

    public void validate() throws IllegalArgumentException {
        if (batchSizeOfOnceReceive < 1 || batchSizeOfOnceReceive > 10) {
            log.warn("The batchSizeOfOnceReceive: {} should be [1,10], using default {}.", batchSizeOfOnceReceive,
                    DEFAULT_BATCH_SIZE_OF_ONCE_RECEIVE);
            batchSizeOfOnceReceive = 1;
        }
        if (numberOfConsumers < 1 || numberOfConsumers > 50) {
            log.warn("The numberOfConsumers: {} should be [1,50], using default {}.", numberOfConsumers,
                    DEFAULT_NUMBER_OF_SQS_CONSUMERS);
            numberOfConsumers = 1;
        }
    }

    public AmazonSQSBufferedAsyncClient buildAmazonSQSClient(AwsCredentialProviderPlugin credPlugin) {
        AmazonSQSAsyncClientBuilder builder = AmazonSQSAsyncClientBuilder.standard();
        QueueBufferConfig config = new QueueBufferConfig()
                .withMaxBatchSize(QueueBufferConfig.MAX_BATCH_SIZE_DEFAULT)
                .withMaxInflightOutboundBatches(
                        QueueBufferConfig.MAX_INFLIGHT_OUTBOUND_BATCHES_DEFAULT * numberOfConsumers);
        if (!this.getAwsEndpoint().isEmpty()) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    this.getAwsEndpoint(),
                    this.getAwsRegion()));
        } else if (!this.getAwsRegion().isEmpty()) {
            builder.setRegion(this.getAwsRegion());
        }
        builder.setCredentials(credPlugin.getCredentialProvider());
        return new AmazonSQSBufferedAsyncClient(builder.build(), config);
    }

}
