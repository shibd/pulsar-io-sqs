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

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.ecosystem.io.sqs.convert.pojo.ExampleMessage;
import org.apache.pulsar.ecosystem.io.sqs.convert.pojo.TimeMessage;
import org.junit.Test;
/**
 * Avro record convert test.
 */
public class AvroRecordConverterTest extends RecordConverterTest {

    @Override
    protected Pair<Schema<GenericRecord>, GenericRecord> getRecordAndSchema() {
        ExampleMessage mockExampleMessage = ExampleMessage.getMockExampleMessage();
        Schema<ExampleMessage> avroSchema = Schema.AVRO(ExampleMessage.class);
        GenericAvroSchema genericAvroSchema = new GenericAvroSchema(avroSchema.getSchemaInfo());
        byte[] encode = avroSchema.encode(mockExampleMessage);
        GenericRecord genericRecord = genericAvroSchema.decode(encode);
        return Pair.of(genericAvroSchema, genericRecord);
    }

    @Test
    public void testAvroLogicalTypeConvert() throws IllegalAccessException, RecordConvertException, IOException {
        testLogicalTypeConvert(Schema.AVRO(TimeMessage.class));
    }
}
