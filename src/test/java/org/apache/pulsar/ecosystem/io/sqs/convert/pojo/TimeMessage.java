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
package org.apache.pulsar.ecosystem.io.sqs.convert.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TimeMessage {
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"int\",\n"
            + "  \"logicalType\": \"date\"\n"
            + "}")
    private int date;
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"int\",\n"
            + "  \"logicalType\": \"time-millis\"\n"
            + "}")
    private int timeMillis;
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"long\",\n"
            + "  \"logicalType\": \"time-micros\"\n"
            + "}")
    private long timeMicros;
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"long\",\n"
            + "  \"logicalType\": \"timestamp-millis\"\n"
            + "}")
    private long timestampMillis;
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"long\",\n"
            + "  \"logicalType\": \"timestamp-micros\"\n"
            + "}")
    private long timestampMicros;
    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"long\",\n"
            + "  \"logicalType\": \"local-timestamp-millis\"\n"
            + "}")
    private long localTimestampMillis;

    @org.apache.avro.reflect.AvroSchema("{\n"
            + "  \"type\": \"long\",\n"
            + "  \"logicalType\": \"local-timestamp-micros\"\n"
            + "}")
    private long localTimestampMicros;
    private InnerMessage innerMessage;

    @Data
    @Builder
    public static class InnerMessage {
        @org.apache.avro.reflect.AvroSchema("{\n"
                + "  \"type\": \"int\",\n"
                + "  \"logicalType\": \"date\"\n"
                + "}")
        private int innerDate;
    }
}
