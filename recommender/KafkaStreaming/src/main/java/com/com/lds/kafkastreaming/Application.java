package com.com.lds.kafkastreaming;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;
import java.util.stream.Stream;

public class Application {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        //定义输入和输出的topic
        String from = "log";
        String to = "recommender_movie";

        //定义kafka streaming 的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"logFilter");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"logFilter");

        StreamsConfig config = new StreamsConfig(settings);

        //拓扑构造器
        TopologyBuilder builder = new TopologyBuilder();
        //定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
               .addProcessor("PROCESS", () -> new LogProcess(), "SOURCE")
                .addSink("SINK", to, "PROCESS");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        System.out.println("kafka streaming started");
    }
}
