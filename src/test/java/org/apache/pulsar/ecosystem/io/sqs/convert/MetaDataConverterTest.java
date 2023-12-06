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
package org.apache.pulsar.ecosystem.io.sqs.convert;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class MetaDataConverterTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testMetaDataConvert() {
        MessageId mockMessageId = Mockito.mock(MessageId.class);
        Mockito.when(mockMessageId.toString()).thenReturn("1:1:123");
        Message mockMessage = Mockito.mock(Message.class);
        Mockito.when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        Mockito.when(mockMessage.getSequenceId()).thenReturn(0L);
        String dateText = "2023-10-30T06:13:48.123+08:00";
        ZonedDateTime zonedDateTime =
                ZonedDateTime.parse(dateText, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
        Instant instant = zonedDateTime.toInstant().plusNanos(456789);
        long mockEventTime = instant.toEpochMilli();
        String topic = "my-topic";
        String key = "my-key";
        Record<GenericRecord> record = new Record() {
            @Override
            public Schema<GenericRecord> getSchema() {
                return null;
            }

            @Override
            public GenericRecord getValue() {
                return null;
            }

            @Override
            public Optional<Message<GenericRecord>> getMessage() {
                return Optional.of(mockMessage);
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(mockEventTime);
            }

            @Override
            public Optional<String> getTopicName() {
                return Optional.of(topic);
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of(key);
            }

            @Override
            public Optional<Integer> getPartitionIndex() {
                return Optional.of(0);
            }

            @Override
            public Map<String, String> getProperties() {
                return Map.of("key1", "value1", "key2", "value2");
            }
        };
        HashSet<String> metaDataFields = new HashSet<>();
        metaDataFields.add("pulsar.topic");
        metaDataFields.add("pulsar.key");
        metaDataFields.add("pulsar.partitionIndex");
        metaDataFields.add("pulsar.sequence");
        metaDataFields.add("pulsar.eventTime");
        metaDataFields.add("pulsar.messageId");
        metaDataFields.add("pulsar.properties.key1");
        metaDataFields.add("pulsar.properties.key2");
        MetaDataConverter metaDataConverter = new MetaDataConverter(metaDataFields);
        Map<String, MessageAttributeValue> attribute = metaDataConverter.convert(record);

        Assert.assertEquals(topic, attribute.get("pulsar.topic").getStringValue());
        Assert.assertEquals(key, attribute.get("pulsar.key").getStringValue());
        Assert.assertEquals("0", attribute.get("pulsar.partitionIndex").getStringValue());
        Assert.assertEquals("0", attribute.get("pulsar.sequence").getStringValue());
        Assert.assertEquals(dateText, attribute.get("pulsar.eventTime").getStringValue());
        Assert.assertEquals("1:1:123", attribute.get("pulsar.messageId").getStringValue());
        Assert.assertEquals("value1", attribute.get("pulsar.properties.key1").getStringValue());
        Assert.assertEquals("value2", attribute.get("pulsar.properties.key2").getStringValue());
    }
}
