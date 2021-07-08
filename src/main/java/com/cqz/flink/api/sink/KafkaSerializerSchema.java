package com.cqz.flink.api.sink;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaSerializerSchema implements KafkaSerializationSchema<String> {

    private String topic;

    public KafkaSerializerSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {

        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
    }


}
