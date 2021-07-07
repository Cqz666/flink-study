//package com.cqz.flink.api.source;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//
//import java.util.Properties;
//
//public class SourceTest4Kafka {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","localhost:9092");
//        properties.setProperty("group.id","consumer-group");
//        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        DataStream<String> source = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
//        source.print();
//        env.execute();
//
//    }
//}
