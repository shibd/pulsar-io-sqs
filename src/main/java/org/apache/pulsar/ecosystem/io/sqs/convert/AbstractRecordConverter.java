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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * Abstract record convert impl, that convert metadata.
 */
@Slf4j
public abstract class AbstractRecordConverter implements RecordConverter {

    protected static final TypeReference<Map<String, Object>> TYPEREF = new TypeReference<>() {
    };
    protected static final String VALUE_KEY = "value";

    protected final ObjectMapper mapper;

    public AbstractRecordConverter() {
        this.mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    protected static String convertLogicalTimeField(LogicalType logicalType, long value) {
        return switch (logicalType.getName()) {
            case "date" -> LocalDate.ofEpochDay(value).format(DateTimeFormatter.ISO_LOCAL_DATE);
            case "time-millis" -> LocalTime.ofNanoOfDay(value * 1_000_000L)
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
            case "time-micros" -> LocalTime.ofNanoOfDay(value * 1_000L)
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
            case "timestamp-millis" -> Instant.ofEpochMilli(value).atZone(ZoneId.systemDefault())
                    .format(DateTimeFormatter.ofPattern(
                            "yyy-MM-dd'T'HH:mm:ss.SSSXXX"));
            case "timestamp-micros" -> Instant.ofEpochSecond(value / 1_000_000).plusNanos((value % 1_000_000) * 1_000)
                    .atZone(ZoneId.systemDefault())
                    .format(DateTimeFormatter.ofPattern(
                            "yyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"));
            case "local-timestamp-millis" -> LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
            case "local-timestamp-micros" -> LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(value / 1_000_000).plusNanos((value % 1_000_000) * 1_000),
                            ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
            default -> String.valueOf(value);
        };
    }

    protected static void convertFields(Record<GenericRecord> record, ObjectNode jsonNode) {
        Schema schema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());
        convertFields(schema, null, null, -1, jsonNode);
    }

    private static void convertFields(Schema schema, ObjectNode parentNode, String fieldName, int arrayIndex,
                                      JsonNode jsonNode) {
        switch (schema.getType()) {
            case RECORD -> {
                ObjectNode objectNode = (ObjectNode) jsonNode;
                for (Schema.Field field : schema.getFields()) {
                    JsonNode node = objectNode.get(field.name());
                    if (node != null) {
                        convertFields(field.schema(), objectNode, field.name(), -1, node);
                    }
                }
            }
            case ARRAY -> {
                ArrayNode arrayNode = (ArrayNode) jsonNode;
                Schema elementSchema = schema.getElementType();
                for (int i = 0; i < arrayNode.size(); i++) {
                    convertFields(elementSchema, null, null, i, arrayNode.get(i));
                }
            }
            case UNION -> {
                // If the field is a union, check each type in the union
                for (Schema type : schema.getTypes()) {
                    convertFields(type, parentNode, fieldName, arrayIndex, jsonNode);
                }
            }
            case INT, LONG -> {
                LogicalType logicalType = schema.getLogicalType();
                if (logicalType != null) {
                    String convertedValue = convertLogicalTimeField(logicalType, jsonNode.asLong());
                    if (parentNode != null && fieldName != null) {
                        parentNode.put(fieldName, convertedValue);
                    } else if (jsonNode.isArray() && arrayIndex != -1) {
                        ((ArrayNode) jsonNode).set(arrayIndex, JsonNodeFactory.instance.textNode(convertedValue));
                    }
                }
            }
            default -> {
            }
        }
    }

}
