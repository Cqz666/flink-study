package com.cqz.flink.api.sink;

import com.cqz.flink.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class SinkTest4Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.readTextFile(Objects.requireNonNull(SinkTest4Redis.class.getClassLoader().getResource("source.txt")).toString());

        DataStream<SensorReading> dataStream = input.map(value -> {
            String[] filed = value.split(",");
            return new SensorReading(filed[0], new Long(filed[1]), new Double(filed[2]));
        });
        List<HttpHost> httpHosts =  new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));
        dataStream.addSink(new ElasticsearchSink.Builder<>(httpHosts,new MyEsSink()).build());

        env.execute();
    }

    public static class MyEsSink implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading sensorReading, RuntimeContext ctx, RequestIndexer requestIndexer) {
            //包装数据源
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id",sensorReading.getId());
            dataSource.put("temp",sensorReading.getTemperature().toString());
            dataSource.put("ts",sensorReading.getTimestamp().toString());
            //创建请求，向ES发起写入命令
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);
            //用Index发送请求
            requestIndexer.add(indexRequest);


        }
    }

}
