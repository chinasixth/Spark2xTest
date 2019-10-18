package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class KafkaProcessTime {
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaProcessTime");

    /**
     * 读取kafka数据，按照指定时间间隔进行滚动(系统时间)
     * 对窗口时间（滚动时间间隔）内的数据进行操作
     * 可以实现滑动窗口，同样是对窗口长度内的数据进行操作
     *
     * @param args
     */
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.180:6667,192.168.1.181:6667,192.168.1.182:6667");
        properties.setProperty("group.id", "group_test");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);

        DataStreamSource<String> source = env.addSource(consumer010);

        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String str : value.split(" ")) {
                    out.collect(new Tuple2<>(str, 1));
                }
            }
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
