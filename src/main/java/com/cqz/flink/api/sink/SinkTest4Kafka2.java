package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest4Kafka2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackend stateBackend = new MemoryStateBackend(5*1024*1024*100);
        env.enableCheckpointing(2*1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(stateBackend);
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","consumer-test");
        properties.put("isolation.level", "read_committed");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        DataStream<String> input = env.addSource(new FlinkKafkaConsumer<>("source_1", new SimpleStringSchema(), properties));
        input.print("input");
        DataStream<String> dataStream = input.map( value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2])).toString();
        }).setParallelism(1);

        dataStream.addSink(new FlinkKafkaProducer<>("sink_1",new KafkaSerializerSchema("sink_1"),
                buildProducerProperties(),FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        dataStream.print("output");

        env.execute();
    }

    public static Properties buildProducerProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("transaction.timeout.ms",5*60*1000);
        return props;
    }
}
