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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * Meta data utils.
 */
@Slf4j
public class MetaDataConverter {

    private static final Map<String, Function<Record<GenericRecord>, Optional<String>>> metaDataConvert =
            new HashMap<>();
    private static final String PROPERTIES_PREX = "pulsar.properties";

    static {
        metaDataConvert.put("pulsar.topic", (record, ctx) -> record.getTopicName().map(String::toString));
        metaDataConvert.put("pulsar.key", (record, ctx) -> record.getKey().map(String::toString));
        metaDataConvert.put("pulsar.partitionIndex",
                (record, ctx) -> record.getPartitionIndex().map(v -> String.valueOf(v)));
        metaDataConvert.put("pulsar.sequence",
                (record, ctx) -> record.getMessage().map(Message::getSequenceId).map(v -> String.valueOf(v)));
        metaDataConvert.put("pulsar.eventTime",
                (record, ctx) -> record.getEventTime().map(epochMs -> Instant.ofEpochMilli(epochMs).atZone(
                        ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(
                        "yyy-MM-dd'T'HH:mm:ss.SSSXXX"))));
        metaDataConvert.put("pulsar.messageId",
                (record, ctx) -> record.getMessage().map(msg -> msg.getMessageId().toString()));
    }

    private final Set<String> metaDataFields;

    public MetaDataConverter(Set<String> metaDataFields) {
        this.metaDataFields = metaDataFields;
    }

    public static boolean isSupportMetaData(String fieldName) {
        if (fieldName.contains(PROPERTIES_PREX)) {
            return true;
        }
        return metaDataConvert.containsKey(fieldName);
    }

    /**
     * Convert meta data.
     * @param record
     * @return
     */
    public Map<String, MessageAttributeValue> convert(Record<GenericRecord> record) {
        Map<String, MessageAttributeValue> attributeMap = new HashMap<>();
        Map<String, String> properties = record.getProperties();
        for (String metaDataField : metaDataFields) {
            // Special handling of properties
            if (metaDataField.contains(PROPERTIES_PREX)) {
                String propertiesKey = metaDataField.replace(PROPERTIES_PREX + ".", "");
                String propertiesValue = properties.get(propertiesKey);
                if (!StringUtils.isEmpty(propertiesValue)) {
                    attributeMap.put(metaDataField,
                            new MessageAttributeValue().withDataType("String").withStringValue(propertiesValue));
                } else {
                    log.warn("Properties value for key {} is empty", propertiesKey);
                }
            } else {
                convert(metaDataField, record).ifPresent(v -> attributeMap.put(metaDataField,
                        new MessageAttributeValue().withDataType("String")
                                .withStringValue(v)));
            }
        }
        return attributeMap;
    }

    /**
     * Convert meta data field.
     *
     * @return If convert failed, will be return optional.empty().
     */
    private Optional<String> convert(String fieldName, Record<GenericRecord> record) {
        try {
            return metaDataConvert.get(fieldName).process(record, null);
        } catch (Exception e) {
            log.warn("Failed to convert meta data field: {}, this field is ignored", fieldName, e);
            return Optional.empty();
        }
    }
}
