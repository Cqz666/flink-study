package com.cqz.flink.api.state;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class TestKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });
        //增量窗口
        DataStream<Integer> resultStream = dataStream.keyBy("id").map(new MyKeyCountMapper());

        resultStream.print();
        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer>{

        private ValueState<Integer> keyedCountState ;
        private ListState<String> listState;
        private MapState<String,Double> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyedCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count",Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-state",String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map-state",String.class,Double.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = keyedCountState.value();
            if (count==null){
                count=0;
            }
            count++;
            keyedCountState.update(count);
            //其他状态Api调用
            //list state
            for (String str : listState.get()){
                System.out.println(str);
            }
            listState.add("test");
            //map state
            mapState.put("1",211.1);
            mapState.get("1");
            mapState.clear();

            return count;
        }
    }

}
