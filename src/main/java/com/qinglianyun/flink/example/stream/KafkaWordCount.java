package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class KafkaWordCount {
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaWordCount");

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * 开启checkpoint，用作故障恢复
         * 注意：目前不支持迭代流启动检查点，可以重写此机制
         * */
        env.enableCheckpointing(5000);

        /*
         * kafka 自带事件时间，
         * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
         *
         * */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.180:6667,192.168.1.181:6667,192.168.1.182:6667");
        properties.setProperty("group.id", "group_test");
        /*
         * 默认情况下禁用分区发现，
         * 如果要启动分区发现，设置一个非负值，表示分区发现时间间隔
         *
         * */
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        /*
         * kafka 消费的起始位置
         * setStartFromEarliest();     // start from the earliest record possible
         * setStartFromLatest();       // start from the latest record；从指定时间戳开始读取数据，对于每个分区，其时间戳大于等于指定时间戳的record将作为开始的位置；
         *                                 如果分区的最新纪律小于指定时间戳，则将仅从最新的记录开始读取数据，在这种模式下，kafka提交的偏移量将被忽略
         * setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
         * setStartFromGroupOffsets(); // default，从记录的偏移量位置开始读取数据，如果找不到偏移量，则根据auto.offset.reset属性设置
         *
         * setStartFromSpecificOffsets(); 指定具体分区的偏移量
         *
         * setCommitOffsetsOnCheckpoints(); 当检查点完成时，是否提交offset，默认为true，可以保证已经提交的偏移量和检查点的偏移量一致；如此将会忽略自动定期提交offset
         * */
        HashMap<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("test", 0), 1L);
        specificStartOffsets.put(new KafkaTopicPartition("test", 1), 2L);
        specificStartOffsets.put(new KafkaTopicPartition("test", 2), 3L);

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties));

        /*
        * flink 有两种状态: 被key化的状态和算子状态
        * 被key化的状态和算子又有两种形式：托管状态和原始状态
        * 托管状态是由flink运行时控制的数据结构表示
        * 原始状态是算子保存在自己的数据结构中的状态
        *
        * 对于non-keyed 原始数据流，不会被切分成多个逻辑数据流，并且所有的窗口逻辑将由单个的task执行
        * 一般先对流进行逻辑分区，然后定义一个window assigner
        * */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String str : value.split(" ")) {
                    out.collect(new Tuple2(str, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            LOGGER.error("flink application execute failure......" + e);
        }

    }
}
