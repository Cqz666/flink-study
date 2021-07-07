package com.cqz.flink.api.demo;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class MyBroadcastStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置为4
        env.setParallelism(4);
        // 广播流数据的载体描述
        final MapStateDescriptor<String, String> broadcastDesc = new MapStateDescriptor<>(
                "broadcast-char",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        // 广播流: 每5秒更新广播(1个随机字母)
        BroadcastStream<String> broadcastStream = env.addSource(new RichSourceFunction<String>() {
            private String[] dataSet = new String[]{"a", "b", "c", "d"};

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = dataSet.length;
                while (true) {
                    TimeUnit.SECONDS.sleep(5);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(dataSet[seed]);
                }
            }

            @Override
            public void cancel() {
            }
            // 一个广播流可同时支持多个广播数据的载体描述(也就是可以同时广播多种类型的数据)，必须都是MapStateDescriptor类型
        }).setParallelism(1).broadcast(broadcastDesc);
        // 主数据流: 每1秒发送一个数字
        DataStream<String> dataStream = env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    TimeUnit.MILLISECONDS.sleep(1000);
                    int seed = (int) (Math.random() * 100D);
                    ctx.collect(String.valueOf(seed));
                }
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(1);

        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, String>() {
            private String keywords = null;
            /*
             * open方法有默认实现，此处可以省略。如果系统启动时需要先做一些事情，可以在此方法中实现。
             *
             * 如果要是想启动的时候就加载初始化数据，注意open方法不能将这些数据保存在Context中(flink设计的
             * 初衷是通过checkpoint来保障数据，所以没考虑每次启动时都要加载数据)，所以就必须使用临时变量了
             * (除了不能进checkpoint，对流功能结果表现是一致的)。
             *
             * 注意，open方法在每个taskJob中都会执行，可以通过下面的Math.random()验证。
             */

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keywords = "x" + Math.random();
            }
            /*
             * 处理主数据流的方法processElement在每个taskJob中都会执行
             */
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                out.collect(keywords);
                out.collect(String.valueOf(ctx.getBroadcastState(broadcastDesc).get("keywords")));
            }
            /*
             * 处理广播流数据的方法processBroadcastElement在每个taskJob中都会执行，可以通过下面的Math.random()验证。
             */
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> collector) throws Exception {
                // 将广播数据保存为临时变量
                keywords = value + Math.random();
                // 将广播数据保存到Context：此时将记录进checkpoint中
                ctx.getBroadcastState(broadcastDesc).put("keywords", "ctx:"+keywords);

            }
        }).print();

    env.execute();
//        不同taskjob输出的x后随机数不同证明open方法在每个taskjob中都被执行，同一taskjob输出结果一致证明open只被执行一次。
//        不同taskjob输出的d后随机数不同证明processBroadcastElement方法在每个taskjob中都被执行，同一taskjob输出结果一致证明processBroadcastElement只被执行一次。
    }
}
