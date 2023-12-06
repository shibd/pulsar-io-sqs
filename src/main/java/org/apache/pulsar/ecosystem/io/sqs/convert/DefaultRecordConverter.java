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

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * Manage all record convert.
 */
public class DefaultRecordConverter implements RecordConverter {

    private final Map<SchemaType, RecordConverter> structConverters;
    private final PrimitiveRecordConverter primitiveRecordConverter;

    public DefaultRecordConverter() {
        structConverters = new HashMap<>();

        this.primitiveRecordConverter = new PrimitiveRecordConverter();
        structConverters.put(SchemaType.JSON, new JsonRecordConverter());
        structConverters.put(SchemaType.AVRO, new AvroRecordConverter());
    }

    private RecordConverter getRecordConverter(SchemaType schemaType) throws RecordConvertException {
        if (schemaType.isPrimitive()) {
            return this.primitiveRecordConverter;
        }
        RecordConverter recordConverter = structConverters.get(schemaType);
        if (recordConverter == null) {
            throw new RecordConvertException("Not support schema type: " + schemaType);
        }
        return recordConverter;
    }

    @Override
    public String convertToJson(Record<GenericRecord> record) throws RecordConvertException {
        GenericRecord value = record.getValue();
        RecordConverter recordConverter = getRecordConverter(value.getSchemaType());
        try {
            return recordConverter.convertToJson(record);
        } catch (RecordConvertException e) {
            throw e;
        } catch (Exception e) {
            throw new RecordConvertException("Record convert failed: " + e.getMessage(), e);
        }
    }
}
