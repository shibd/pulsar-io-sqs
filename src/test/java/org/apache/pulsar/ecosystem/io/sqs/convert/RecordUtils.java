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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;

public class RecordUtils {
    public static <T> Record<GenericRecord> buildGenericRecord(T pojo, Class<T> clazz, Schema<T> schema)
            throws IllegalAccessException {
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schema.getSchemaInfo());

        // Build the generic record
        GenericRecordBuilder recordBuilder = genericSchema.newRecordBuilder();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            recordBuilder.set(field.getName(), field.get(pojo));
        }
        return new Record<>() {
            @Override
            public GenericRecord getValue() {
                return recordBuilder.build();
            }

            @Override
            public Schema<GenericRecord> getSchema() {
                return new Schema<>() {
                    @Override
                    public byte[] encode(GenericRecord message) {
                        return new byte[0];
                    }

                    @Override
                    public SchemaInfo getSchemaInfo() {
                        return schema.getSchemaInfo();
                    }

                    @Override
                    public Schema<GenericRecord> clone() {
                        return null;
                    }
                };
            }

            @Override
            public Optional<String> getTopicName() {
                return Optional.of("test-topic");
            }

            @Override
            public Map<String, String> getProperties() {
                Map<String, String> result = new HashMap<>();
                result.put("key1", "value1");
                result.put("key2", "value2");
                return result;
            }
        };
    }
}
