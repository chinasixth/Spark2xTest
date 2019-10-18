package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class FlinkWatermark {
    private static final Logger LOGGER = LoggerFactory.getLogger("FlinkWatermark");

    public static void main(String[] args) {

        /*
         * 使用 watermark 需要3个步骤
         * 1. 对timestamp进行提取，即调用
         * 2. 实例化
         * 3. 定义时间窗口：窗口长度(当窗口长度等于滑动窗口即为翻滚窗口)和滑动窗口
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 检查点默认是关闭的
        env.enableCheckpointing(5000);
        /*
         * 如果使用事件事件，一定要从事件中提取出事件，否则，程序不报错，也不中止
         * */
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.180:6667,192.168.1.181:6667,192.168.1.182:6667");
        properties.setProperty("group.id", "group_test");
        /*
         * 读取 kafka message的元数据信息，如：topic、partition、offset等相关信息
         * 注意：ConsumerRecord中的key和value都是bytes[]类型
         *      在获取具体值的时候，需要对byte[]进行强制类型转换
         * 通常情况下，用watermark处理乱序的情况比较多
         *
         *
         * */
        FlinkKafkaConsumer010<ConsumerRecord> consumer010 = new FlinkKafkaConsumer010<>("test", new KafkaDeserializationSchema<ConsumerRecord>() {
            @Override
            public boolean isEndOfStream(ConsumerRecord nextElement) {
                return false;
            }

            @Override
            public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                return record;
            }

            @Override
            public TypeInformation<ConsumerRecord> getProducedType() {
                return TypeInformation.of(ConsumerRecord.class);
            }
        }, properties);

        /*
         * flink 提供了两个预定义实现类去生成水印
         * AscendingTimestampExtractor 适用于时间戳递增的情况
         * BoundedOutOfOrdernessTimestampExtractor 适用于乱序但最大延迟已知的情况
         *
         * 还可以根据需要自定义timestamp和watermark
         * AssignerWithPunctuatedWatermarks： 数据流中每一个递增的EventTime都会产生一个Watermark。
         * 在实际的生产中，在TPS很高的场景下会产生大量的Watermark在一定程度上对下游算子造成压力，所以只有在实时性要求非常高的场景才会选择间断的方式进行水印的生成。
         * AssignerWithPeriodicWatermarks： 周期性的（一定时间间隔或者达到一定的记录条数）产生一个Watermark。
         * 在实际的生产中定期的方式必须结合时间和积累条数两个维度继续周期性产生Watermark，否则在极端情况下会有很大的延时。所以水印的生成方式需要根据业务场景的不同进行不同的选择。
         * */
        consumer010.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ConsumerRecord>() {
            long currentMaxTimestamp = Long.MIN_VALUE;
            long maxOutOfOrderness = 10000L; // 允许最大的乱序时间是10s

            @Override
            public Watermark checkAndGetNextWatermark(ConsumerRecord lastElement, long extractedTimestamp) {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(ConsumerRecord element, long previousElementTimestamp) {
                long timestamp = Long.parseLong(new String((byte[]) element.value()).split(",")[0]);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        DataStreamSource<ConsumerRecord> source = env.addSource(consumer010);

//        SingleOutputStreamOperator<ConsumerRecord> watermarks = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ConsumerRecord>(Time.seconds(2)) {
//            @Override
//            public long extractTimestamp(ConsumerRecord element) {
//                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//                System.out.println(format.format(Long.parseLong(new String((byte[]) element.value()).split(",")[0])));
//                return Long.parseLong(new String((byte[]) element.value()).split(",")[0]);
//            }
//        });

        SingleOutputStreamOperator<String> string = source.map(new MapFunction<ConsumerRecord, String>() {
            @Override
            public String map(ConsumerRecord value) throws Exception {
                return new String((byte[]) value.value());
            }
        });

        DataStream<Tuple2<String, Integer>> words = string.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String str : value.split(",")[1].split(" ")) {
                    out.collect(new Tuple2<>(str, 1));
                }
            }
        });

        words.keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .timeWindow(Time.seconds(3))
                .sum(1)
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void consumerRecordGeneric() {
        /*
         * 想直接将把kafka中的数据读取成ConsumerRecord<String, String>格式
         * 状态：failure
         * */
//        FlinkKafkaConsumer010<ConsumerRecord<String, String>> consumerRecordFlinkKafkaConsumer010 = new FlinkKafkaConsumer010<>("test", new KafkaDeserializationSchema<ConsumerRecord<String, String>>() {
//            @Override
//            public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
//                return false;
//            }
//
//            @Override
//            public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
//                return new ConsumerRecord<String, String>(record.topic(), record.partition(), record.offset(),
//                        new String(record.key()), new String(record.value()));
//            }
//
//            @Override
//            public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
//                return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
//                });
//            }
//        }, properties);
//
//        DataStreamSource<ConsumerRecord<String, String>> source = env.addSource(consumerRecordFlinkKafkaConsumer010);
//        source.print();
    }

    public static void simpleString() {
        /*
         * 只能读取kafka的value，序列化为String
         * */
//        FlinkKafkaConsumerBase<String> kafkaConsumerBase = new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(), properties)
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
//                    @Nullable
//                    @Override
//                    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
//                        return new Watermark(Long.parseLong(lastElement.split(",")[0]));
//                    }
//
//                    @Override
//                    public long extractTimestamp(String element, long previousElementTimestamp) {
//                        return Long.parseLong(element.split(",")[0]);
//                    }
//                });

//        DataStreamSource<String> stream = env.addSource(kafkaConsumerBase);
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> words = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String data = value.split(",")[1];
//                for (String str : data.split(" ")) {
//                    out.collect(new Tuple2<>(str, 1));
//                }
//            }
//        });
    }

}
