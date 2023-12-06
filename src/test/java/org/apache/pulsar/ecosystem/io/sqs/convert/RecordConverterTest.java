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

import static org.junit.Assert.assertEquals;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.sqs.convert.pojo.TimeMessage;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Record convert test.
 */
public abstract class RecordConverterTest {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected abstract Pair<Schema<GenericRecord>, GenericRecord> getRecordAndSchema();

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertToJson() throws RecordConvertException, JsonProcessingException {
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
        Pair<Schema<GenericRecord>, GenericRecord> recordAndSchema = getRecordAndSchema();
        Record<? extends GenericRecord> record = new Record<GenericRecord>() {
            @Override
            public Schema<GenericRecord> getSchema() {
                return recordAndSchema.getLeft();
            }

            @Override
            public GenericRecord getValue() {
                return recordAndSchema.getRight();
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
        };

        DefaultRecordConverter recordConvert = new DefaultRecordConverter();
        String jsonString = recordConvert.convertToJson((Record<GenericRecord>) record);
        System.out.println(jsonString);
        // assert user data
        JsonNode data = MAPPER.readTree(jsonString);
        Assert.assertNotNull(data);
    }

    public void testLogicalTypeConvert(Schema<TimeMessage> schema)
            throws IllegalAccessException, RecordConvertException, IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        LocalDate localDate = LocalDate.of(2023, 10, 30);
        String localDateText = "2023-10-30";
        LocalTime localTime = LocalTime.of(13, 48, 41, 123000000);
        String localTimeText = "13:48:41.123";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        String dateText = "2023-10-30T06:13:48.123+08:00";
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateText, formatter);
        Instant instant = zonedDateTime.toInstant().plusNanos(456789);
        // For local-timestamp-millis and local-timestamp-micros, we'll use the same Instant
        // but without the timezone offset.
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        long localTimestampMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long localTimestampMicros = localDateTime.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000
                + localDateTime.toInstant(ZoneOffset.UTC).getNano() / 1_000;
        Record<GenericRecord> testRecord =
                RecordUtils.buildGenericRecord(TimeMessage.builder()
                                .date((int) localDate.toEpochDay())
                                .timeMillis((int) (localTime.toNanoOfDay() / 1_000_000L))
                                .timeMicros((localTime.toNanoOfDay() / 1_000L))
                                .timestampMillis(instant.toEpochMilli())
                                .timestampMicros(instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000)
                                .localTimestampMillis(localTimestampMillis)
                                .localTimestampMicros(localTimestampMicros)
                                .innerMessage(TimeMessage.InnerMessage.builder()
                                        .innerDate((int) localDate.toEpochDay())
                                        .build())
                                .build(),
                        TimeMessage.class, schema);

        RecordConverter recordConverter = new DefaultRecordConverter();
        String jsonStr = recordConverter.convertToJson(testRecord);

        JsonNode rootNode = new ObjectMapper().readTree(jsonStr);
        assertEquals(localDateText, rootNode.get("date").asText());
        assertEquals(localDateText, rootNode.get("innerMessage").get("innerDate").asText());
        assertEquals(localTimeText, rootNode.get("timeMillis").asText());
        assertEquals(localTimeText, rootNode.get("timeMicros").asText());
        assertEquals(dateText, rootNode.get("timestampMillis").asText());
        assertEquals("2023-10-30T06:13:48.123456+08:00", rootNode.get("timestampMicros").asText());
        assertEquals("2023-10-29T22:13:48.123", rootNode.get("localTimestampMillis").asText());
        assertEquals("2023-10-29T22:13:48.123456", rootNode.get("localTimestampMicros").asText());
    }
}
