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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;

/**
 * Primitive record convert test.
 */
@Slf4j
public class PrimitiveRecordConverterTest {

    public JsonNode primitiveConvert(SchemaType schemaType, Object nativeObject)
            throws RecordConvertException, IOException {
        Record<GenericRecord> record = () -> new GenericRecord() {
            @Override
            public byte[] getSchemaVersion() {
                return new byte[0];
            }

            @Override
            public List<Field> getFields() {
                return null;
            }

            @Override
            public Object getField(String fieldName) {
                return null;
            }

            @Override
            public SchemaType getSchemaType() {
                return schemaType;
            }

            @Override
            public Object getNativeObject() {
                return nativeObject;
            }
        };
        DefaultRecordConverter recordConvert = new DefaultRecordConverter();
        String jsonString = recordConvert.convertToJson(record);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(jsonString);
        return rootNode.get(AbstractRecordConverter.VALUE_KEY);
    }

    @Test
    public void testPrimitiveConvert() throws RecordConvertException, IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        Random random = new Random();
        byte[] bytes = new byte[10];
        random.nextBytes(bytes);
        String testString = "Abc123!@# XYZ789*()_+{}:\"<>?,./;\'[]\\`~";
        boolean randomBoolean = random.nextBoolean();
        short randomShort = (short) random.nextInt(Short.MAX_VALUE + 1);
        int randomInt = random.nextInt();
        long randomLong = random.nextLong();
        float randomFloat = random.nextFloat();
        double randomDouble = random.nextDouble();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        String dateText = "2023-10-30T06:13:48.123+08:00";
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateText, formatter);
        Instant instant = zonedDateTime.toInstant().plusNanos(456789);
        Date date = Date.from(instant);
        Time time = new Time(instant.toEpochMilli());
        Timestamp timestamp = Timestamp.from(instant);
        LocalDate localDate = LocalDate.of(2023, 10, 30);
        String localDateText = "2023-10-30";
        String localTimeText = "13:48:41.123456789";
        LocalTime localTime = LocalTime.of(13, 48, 41, 123456789);
        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);

        assertEquals(Base64.getEncoder().encodeToString(bytes), primitiveConvert(SchemaType.BYTES, bytes).asText());
        assertEquals(testString, primitiveConvert(SchemaType.STRING, testString).asText());
        assertEquals(randomBoolean, primitiveConvert(SchemaType.BOOLEAN, randomBoolean).asBoolean());
        assertEquals((int) bytes[0], primitiveConvert(SchemaType.INT8, bytes[0]).asInt());
        assertEquals(randomShort, primitiveConvert(SchemaType.INT16, randomShort).asInt());
        assertEquals(randomInt, primitiveConvert(SchemaType.INT32, randomInt).asInt());
        assertEquals(randomLong, primitiveConvert(SchemaType.INT64, randomLong).asLong());
        assertEquals(randomFloat, primitiveConvert(SchemaType.FLOAT, randomFloat).floatValue(), 0.01);
        assertEquals(randomDouble, primitiveConvert(SchemaType.DOUBLE, randomDouble).doubleValue(), 0.01);
        assertEquals(dateText, primitiveConvert(SchemaType.DATE, date).asText());
        assertEquals(dateText, primitiveConvert(SchemaType.TIME, time).asText());
        assertEquals(dateText, primitiveConvert(SchemaType.TIMESTAMP, timestamp).asText());
        assertEquals(localDateText, primitiveConvert(SchemaType.LOCAL_DATE, localDate).asText());
        assertEquals(localTimeText, primitiveConvert(SchemaType.LOCAL_TIME, localTime).asText());
        assertEquals(localDateText + "T" + localTimeText,
                primitiveConvert(SchemaType.LOCAL_DATE_TIME, localDateTime).asText());
        assertEquals("2023-10-30T06:13:48.123456789+08:00", primitiveConvert(SchemaType.INSTANT, instant).asText());
    }
}
