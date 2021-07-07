//package com.cqz.flink.api.transform;
//
//import com.cqz.flink.api.beans.SensorReading;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.ConnectedStreams;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.CoMapFunction;
//import scala.Tuple2;
//import scala.Tuple3;
//
//import java.util.Collections;
//
///**
// *   分 流  bugfix
// */
//public class MultipleStream {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> input = env.readTextFile(EasyOperator.class.getClassLoader().getResource("source.txt").toString());
//
//        DataStream<SensorReading> dataStream = input.map((MapFunction<String, SensorReading>) value -> {
//            String[] filed = value.split(",");
//            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
//        });
//        //切分流
//        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
//            @Override
//            public Iterable<String> select(SensorReading value) {
//                return (value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
//            }
//        });
//        DataStream<SensorReading> highStream = splitStream.select("high");
//        DataStream<SensorReading> lowStream = splitStream.select("low");
//
//        //合流
//        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(SensorReading value) throws Exception {
//                return new Tuple2<>(value.getId(), value.getTemperature());
//            }
//        });
//
//        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams
//                = warningStream.connect(lowStream);
//
//        DataStream<Object> streamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
//            @Override
//            public Object map1(Tuple2<String, Double> value) throws Exception {
//                return new Tuple3<>(value._1(), value._2(), "high temp warning!");
//            }
//
//            @Override
//            public Object map2(SensorReading value) throws Exception {
//                return new Tuple2<>(value.getId(), "normal");
//            }
//        });
//
//        highStream.print("high");
//        lowStream.print("low");
//        streamOperator.print();
//        env.execute();
//    }
//}
