package com.cqz.flink.api.sink;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;


public class KafkaDeserSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String key =null;
        if(record.key() != null){
            key = new String(record.key(), StandardCharsets.UTF_8);
        }
        return new ConsumerRecord<>(record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                TimestampType.CREATE_TIME,
                record.checksum(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                key,
                new String(record.value(), StandardCharsets.UTF_8)
        );
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return  TypeInformation.of(new TypeHint<ConsumerRecord<String,String>>(){});
    }
}
