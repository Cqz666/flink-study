package com.cqz.flink.api.window;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class TestTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> input = env.readTextFile(Objects.requireNonNull(SinkTest4Redis.class.getClassLoader().getResource("source.txt")).toString());
        env.setParallelism(1);
        DataStream<String> input = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });
        //增量窗口
        DataStream<Integer> resultStream = dataStream.keyBy("id").timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer acc, Integer acc1) {
                        return acc + acc1;
                    }
                });

        //全量窗口
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(15))
                .apply((WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>) (tuple, window, input1, out) -> {
                    String id = tuple.getField(0);
                    Long end = window.getEnd();
                    Integer count = IteratorUtils.toList(input1.iterator()).size();
                    out.collect(new Tuple3<>(id, end, count));
                });


        resultStream2.print();

        env.execute();
    }
}
