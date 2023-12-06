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
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.ecosystem.io.sqs.convert.pojo.ExampleMessage;
import org.apache.pulsar.ecosystem.io.sqs.convert.pojo.TimeMessage;
import org.junit.Test;

/**
 * Json record convert test.
 */
public class JsonRecordConverterTest extends RecordConverterTest {

    @Override
    protected Pair<Schema<GenericRecord>, GenericRecord> getRecordAndSchema() {
        ExampleMessage mockExampleMessage = ExampleMessage.getMockExampleMessage();
        Schema<ExampleMessage> jsonSchema = Schema.JSON(ExampleMessage.class);
        GenericJsonSchema genericJsonSchema = new GenericJsonSchema(jsonSchema.getSchemaInfo());
        byte[] encode = jsonSchema.encode(mockExampleMessage);
        GenericRecord genericRecord = genericJsonSchema.decode(encode);
        return Pair.of(genericJsonSchema, genericRecord);
    }

    @Test
    public void testJsonLogicalTypeConvert() throws RecordConvertException, IOException, IllegalAccessException {
        testLogicalTypeConvert(Schema.JSON(TimeMessage.class));
    }
}
