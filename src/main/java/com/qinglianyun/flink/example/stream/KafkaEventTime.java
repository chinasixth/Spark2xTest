package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class KafkaEventTime {
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaEventTime");

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(2000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.180:6667,192.168.1.181:6667,192.168.1.182:6667");
        properties.setProperty("group.id", "group_test");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);

        DataStreamSource<String> source = env.addSource(consumer010);

        SingleOutputStreamOperator<String> extracted = source.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
            long maxOutOfOrderness = 1000L;
            long currentMaxTimestamp = Long.MIN_VALUE;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
//                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                return new Watermark(extractedTimestamp);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                long timestamp = Long.parseLong(element.split(",")[0]);
                System.out.println("timestamp=" + format.format(timestamp) + " value=" + element);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = extracted.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String str : value.split(",")[1].split(" ")) {
                    out.collect(new Tuple2<>(str, 1));
                }
            }
        });

        /*
        * 此处要注意：
        *   flink的窗口划分是根据时间已经划分好的，窗口的起始位置跟数据没有任何关系
        *   如：窗口长度为5s，第一条数据的时间为timestamp=2019-10-17 19:13:03，那么这条数据将被分配为timestamp=2019-10-17 19:13:00——————timestamp=2019-10-17 19:13:05的窗口内
        *   当遇到数据的时间大于等于timestamp=2019-10-17 19:13:05时，则触发计算，但是触发计算的数据是不会参与本次计算的
        *   按照如上分析，当flink采用事件时间计算，且指定了watermark，而数据特别少（每小时一条数据），那么将会出现数据延迟的情况。
        *   这个时候可以采用Trigger来触发计算函数
        * */
        words.keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
