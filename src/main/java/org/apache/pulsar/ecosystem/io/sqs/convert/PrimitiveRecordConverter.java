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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * Primitive record convert impl.
 */
@Slf4j
public class PrimitiveRecordConverter extends AbstractRecordConverter {

    public PrimitiveRecordConverter() {
        super();
    }

    @Override
    public String convertToJson(Record<GenericRecord> record) throws RecordConvertException {
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE_KEY, convertField(record));
        try {
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RecordConvertException(e.getMessage(), e);
        }
    }

    private Object convertField(Record<GenericRecord> record) throws RecordConvertException {
        SchemaType type = record.getValue().getSchemaType();
        Object nativeObject = record.getValue().getNativeObject();
        switch (type) {
            case NONE:
            case BYTES:
            case STRING:
            case BOOLEAN:
            case DOUBLE:
            case INT64:
                return nativeObject;
            case INT8:
                if (nativeObject instanceof Byte byteValue) {
                    return byteValue.longValue();
                }
                break;
            case INT16:
                if (nativeObject instanceof Short shortValue) {
                    return shortValue.longValue();
                }
                break;
            case INT32:
                if (nativeObject instanceof Integer intValue) {
                    return intValue.longValue();
                }
                break;
            case FLOAT:
                if (nativeObject instanceof Float floatValue) {
                    return floatValue.doubleValue();
                }
                break;
            case DATE:
                if (nativeObject instanceof Date date) {
                    return date.toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(
                            "yyy-MM-dd'T'HH:mm:ss.SSSXXX"));
                }
                break;
            case TIME:
                if (nativeObject instanceof Time time) {
                    return Instant.ofEpochMilli(time.getTime()).atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern(
                                    "yyy-MM-dd'T'HH:mm:ss.SSSXXX"));
                }
                break;
            case TIMESTAMP:
                if (nativeObject instanceof Timestamp timestamp) {
                    return timestamp.toInstant().atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern(
                                    "yyy-MM-dd'T'HH:mm:ss.SSSXXX"));
                }
                break;
            case LOCAL_DATE:
                if (nativeObject instanceof LocalDate localDate) {
                    return localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
                }
                break;
            case LOCAL_TIME:
                if (nativeObject instanceof LocalTime localTime) {
                    return localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS"));
                }
                break;
            case LOCAL_DATE_TIME:
                if (nativeObject instanceof LocalDateTime localDateTime) {
                    return localDateTime.format(DateTimeFormatter.ofPattern("yyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"));
                }
                break;
            case INSTANT:
                if (nativeObject instanceof Instant instant) {
                    return instant.atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX"));
                }
                break;
        }
        throw new RecordConvertException("Not support type: " + type
                + ", data class: " + nativeObject.getClass().getName());

    }

}
