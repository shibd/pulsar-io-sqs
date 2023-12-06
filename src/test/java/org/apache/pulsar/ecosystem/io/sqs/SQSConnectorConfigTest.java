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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test {@link SQSConnectorConfig}.
 */
public class SQSConnectorConfigTest {

    /*
     * Test Case: load the configuration from an empty property map.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLoadEmptyPropertyMap() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        SQSConnectorConfig.load(emptyMap, Mockito.mock(SinkContext.class));
    }

    /*
     * Test Case: load the configuration from a property map.
     */
    @Test
    public void testLoadPropertyMap() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("awsRegion", "us-east-1");
        properties.put("queueName", "test-queue");
        properties.put("awsEndpoint", "https://some.endpoint.aws");
        properties.put("awsCredentialPluginParam", "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}");
        properties.put("batchSizeOfOnceReceive", 10);
        properties.put("numberOfConsumers", 20);

        SQSConnectorConfig config = SQSConnectorConfig.load(properties, Mockito.mock(SourceContext.class));
        assertEquals("Mismatched Region : " + config.getAwsRegion(),
                "us-east-1", config.getAwsRegion());
        assertEquals("Mismatched queueName : " + config.getQueueName(),
                "test-queue", config.getQueueName());
        assertEquals("Mismatched awsEndpoint : " + config.getAwsEndpoint(),
                "https://some.endpoint.aws", config.getAwsEndpoint());
        assertEquals("Mismatched awsCredentialPluginParam : " + config.getAwsCredentialPluginParam(),
                "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}", config.getAwsCredentialPluginParam());
        assertEquals("Mismatched batchSizeOfOnceReceive: " + config.getBatchSizeOfOnceReceive(),
                10, config.getBatchSizeOfOnceReceive());
        assertEquals("Mismatched numberOfConsumers: " + config.getNumberOfConsumers(),
                20, config.getNumberOfConsumers());
    }

    /*
     * Test Case: init source connector without required params.
     */
    @Test
    public final void testMissingCredentialParam() {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("awsEndpoint", "https://some.endpoint.aws");
        properties.put("awsRegion", "us-east-1");
        properties.put("queueName", "test-queue");

        try {
            SQSSource source = new SQSSource();
            source.open(properties, null);
        } catch (Exception ex) {
            assertNotNull("Missing param should lead to exception", ex);
        }
    }

    /*
     * Test Case: Load awsCredentialPluginParam from secret.
     */
    @Test
    public final void testLoadConfigFromSecret() {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("awsRegion", "us-east-1");
        properties.put("queueName", "test-queue");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        Mockito.when(sinkContext.getSecret("awsCredentialPluginParam")).thenReturn("mock-credential");

        SQSConnectorConfig config = SQSConnectorConfig.load(properties, sinkContext);
        assertEquals("us-east-1", config.getAwsRegion());
        assertEquals("test-queue", config.getQueueName());
        assertEquals("mock-credential", config.getAwsCredentialPluginParam());
    }

}
