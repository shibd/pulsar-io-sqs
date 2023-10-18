## Improve data format for AWS SQS sink connector
This proposal improve data format for AWS SQS sink connector

## Background knowledge

### Parts of an SQS Message

A message in Amazon SQS primarily consists of message attributes and a message body:

- [Message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes):
  Message attributes allow the producer to provide structured metadata items (such as timestamps, strings, numbers, etc.) along with the message body. Each message can have up to 10 attributes.
- [Message body](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html):
  A message body can include only XML, JSON, and unformatted text. The following Unicode characters are allowed: `#x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF`
  Any characters not included in this list are rejected. For more information, see the [W3C specification for characters](https://www.w3.org/TR/REC-xml/#charsets).

Note that the total size of the `message attributes` and the `message body` cannot exceed 256KB.

In the SQS consumer, the following code can be used to extract both the `message attributes` and the `message body`:
```java
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(request);
        for (Message message : receiveMessageResult.getMessages()) {
            Map<String, String> attributes = message.getAttributes();
            String body = message.getBody();
        }
```

Regarding the Apache Pulsar SQS sink connector scenarios:
- We can put the metadata from Pulsar to `message attributes` of AWS SQS, such as TopicName, MessageId, SequenceId, etc.
- Pulsar supports schemas, while SQS does not, and since SQS only supports data storage in the W3C characters, we cannot directly send Pulsar's binary data to SQS. The recommended approach is to convert it into JSON.

## Motivation

### Issues with schema messages conversion
Currently, the AWS SQS sink connector does not handle message convert with schema; it simply calls the `nativeObject.toString()` method. Refer to:

https://github.com/streamnative/pulsar-io-sqs/blob/7b6eb2bc78d6ba0ee7ab5c6e18d2e60ba9ffada5/src/main/java/org/apache/pulsar/ecosystem/io/sqs/SQSSink.java#L103-L108

This is a bug. That will send the address of the `nativeObject` to AWS SQS, such as: `[B@49dfbf49`.

### Issues with metadata conversion
Currently, this connector only processes pulsar metadata sparingly. Just put the `MessageKey` and `Properties` into the `message attribute`. Refer to:

https://github.com/streamnative/pulsar-io-sqs/blob/7b6eb2bc78d6ba0ee7ab5c6e18d2e60ba9ffada5/src/main/java/org/apache/pulsar/ecosystem/io/sqs/SQSSink.java#L116-L118

The problem with this approach is that users cannot flexibly customize the metadata they want.

## Goals

### In Scope
- Support AVRO, JSON, PROTOBUF, Primitive schema convert to JSON and set it to `message body` of AWS SQS.
- Support customize metadata conversion and set it to `message attributes` of AWS SQS.

### Out of Scope
- None

## High-Level Design

#### Schema data convert

##### For primitive schema:

For the primitive type, the data format is as follows:
```JSON
{
  "value": "test-value"
}

// or

{
  "value": true
}

// or

{
  "value": 1234
}

// or

{
  "value": "2023-10-17"
}
```

> *Note*: For the primitive type, although we can directly convert to a string type, to keep the structure schema consistent,
> It is better to use JSON type uniformly so that on the consumer side, users can use JSON tools to simultaneously parse different types of data.

The value types include Number, Boolean, and String.

- For `Boolean`: It's converted to a boolean type in JSON.
- For `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT`, and `DOUBLE`: The value is converted to a string representation of the number.
- For `STRING`: The value is converted to a string type.
- Fort `BYTES`: The value is converted to a base64-encoded string.
- For `DATE`, `TIME`, and `TIMESTAMP`: The value is converted to a string with the [ISO 8601](https://www.w3.org/TR/NOTE-datetime) format `YYYY-MM-DDThh:mm:ss.sssTZD`. Eg. '2023-10-17T08:22:11.263Z'.
- For `LocalDate`: The value is converted to a string with the [ISO 8601](https://www.w3.org/TR/NOTE-datetime) format `YYYY-MM-DD` using the [ISO_LOCAL_DATE DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_LOCAL_DATE). Eg. '2023-10-17'.
- For `LocalTime`: The value is converted to a string with the [ISO 8601](https://www.w3.org/TR/NOTE-datetime) format `hh:mm:ss.sssssssss`. Eg. '04:30:33.123456789'.
- For `LocalDateTime`: The value is converted to a string with the [ISO 8601](https://www.w3.org/TR/NOTE-datetime) format `YYYY-MM-DDThh:mm:ss.ssssssss`. Eg. '2023-10-17T04:30:33.123456789'.
- For `Instant`: The value is converted to a string with the [ISO 8601](https://www.w3.org/TR/NOTE-datetime) format `YYYY-MM-DDThh:mm:ss.ssssssssTZD`. Eg. '2023-10-17T04:30:33.123456789Z'.

The benefits of employing the ISO 8601 format for storing time-related values include:

- Enhanced readability: The data is presented in a clear, easy-to-understand manner.
- Standardized formatting: Using a globally recognized standard allows for effortless parsing with existing tools.
- User-friendly: Users can accurately interpret the time value using this format without knowing the data schema beforehand.
- Cross-Language Compatibility: All programming languages support ISO 8601 formatted dates and times. This makes sharing date and time data across different systems and languages very straightforward.
- Ease of Sorting and Comparison: Since ISO 8601 uses a descending order of significance (year, month, day, hour, minute, second), dates and times can be sorted and compared as simple string values.
- Unambiguity: The date and time representation in ISO 8601 is unambiguous, avoiding confusion that can arise from regional and cultural differences. For instance, "2023-10-17" is clearly understood to be the 17th day of October in 2023, not the 10th day of the 17th month.
- Time Zone Information: ISO 8601 can express time zone information, which is very useful when dealing with dates and times across different time zones.


##### For struct schema:
For the struct schema types `JSON`, `AVRO`, and `PROTOBUF_NATIVE` of Apache Pulsar, the value is converted to a JSON string. The conversion rules outlined in the `Primitive schema section` are applied to all primitive type fields within this value object. Nested objects are also supported.

Here is an example:
```JSON
{
  "stringField": "hello",
  "timeField": "2023-10-17T08:22:11.263Z",
  "numberField": 100,
  "valueField": "test-value" 
}
```

##### For non-schema:
Keep the [current logic](https://github.com/streamnative/pulsar-io-sqs/blob/7b6eb2bc78d6ba0ee7ab5c6e18d2e60ba9ffada5/src/main/java/org/apache/pulsar/ecosystem/io/sqs/SQSSink.java#L100-L101) for data without a schema: Retrieve the `byte array` from the message record and convert it into a string using UTF-8 encoding.

#### Customize Pulsar metadata

SQS sink connector will put metadata of Pulsar into SQS `message attributes`. SQS message attributes accommodate various data types such as String, Number, Binary, and so forth.

The supported metadata fields of Pulsar are:

- `topic`: The `string` type of source topic name
- `key`: The `string` type of the message key.
- `partitionIndex`: The `number` type of the topic partition index of the topic.
- `sequence`: The `number` type of the sequence ID.
- `properties`: This is a map, and will unfold this map, placing each key-value pair into the SQS `message attribute`. The type of the key is `string`, and the type of the value is `string`.
- `eventTime`: The event time of the message in the [ISO 8601 format](https://www.w3.org/TR/NOTE-datetime)
- `messageId`: The string representation of a message ID. eg, `"1:1:-1:-1"`

To maintain the [current key definition rules](https://github.com/streamnative/pulsar-io-sqs/blob/39dbd34d84f27887cb682fe5295c91dd2eb5f790/src/main/java/org/apache/pulsar/ecosystem/io/sqs/SQSUtils.java#L30), each metadata key will be prefixed with `pulsar.`

For examples:
```yaml
"pulsar.topic": "test-topic"
"pulsar.key": "test-key"
"pulsar.partitionsIndex": 1
"pulsar.sequence": 100
"pulsar.properties.key1": "test-properties.value1"
"pulsar.properties.key2": "test-properties.value2"
"pulsar.eventTime": "2023-10-17T04:30:33.123456789"
"pulsar.messageId": "1:1:-1:-1"
```
Users can choose the metadata fields through the `metaDataField` configuration. It is a `string array`. And this connector will verify that the number of metadata cannot exceed 10.

For examples:
```yaml
config:
  metaDataField: 
    - pulsar.key
    - pulsar.topic
    - pulsar.properties.key1
```


## Detailed Design

### Design & Implementation Details

Data convert logic will abstract an interface, and each schema type will have a different implementation.

Interface definition:
```java

/**
 * The interface Record convert.
 */
public interface RecordConvert {

    /**
     * Pulsar record convert JSON Object String.
     * @return The JSON Object String.
     */
    String convertToJson(Record<GenericObject> record);

}
```
There are multiple implementations: `AvroSchemaConvert`, `JsonSchemaConvert`, `ProtobufSchemaConvert`, `PrimitiveSchemaConvert`, `NonSchemaConvert`, etc.

That will convert data by the `High-Level Design` section rule.

### Public-facing Changes

#### Public API
- None

#### Binary protocol
- None

#### Configuration

Added the `metaDataField` configurations to the SQSConnectorConfig:

```java
@FieldDoc(
        required = false,
        defaultValue = "[\"pulsar.key\"]",
        help = "The metadata fields to be sent to the SQS message attributes."
                + "Valid values are [\"pulsar.topic\",\"pulsar.key\",\"pulsar.partitionIndex\",\"pulsar" + ".sequence\",\"pulsar.properties.{{Your properties key}}\", \"pulsar.eventTime\"]\""
)
private List<String> metadataFields;
```


## Monitoring
- No changes for this part.

## Security Considerations
- No changes for this part.

## Backward & Forward Compatibility

This feature is forward-compatible but not backward-compatible.

### Upgrade
Just need upgrade versions.

### Revert
Once the upgrade is successful, if want to revert, consumers of SQS need to switch back to the original version of the consumption logic.


