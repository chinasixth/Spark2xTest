package flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class WikipediaAnalysis {

    private static final Logger LOGGER = LoggerFactory.getLogger("WikipediaAnalysis");

    public static void main(String[] args) {
        /*
         * flink 步骤
         * 1. 创建一个StreamExecutionEnvironment（批处理则创建ExecutionEnvironment）
         * 2. 添加一个数据源
         * 3. 处理逻辑
         * */
        try {
            LocalStreamEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();
//            StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

            // 从数据中提取一个字段作为数据的 key
            KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
                @Override
                public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                    return wikipediaEditEvent.getUser();
                }
            });

            DataStream<Tuple2<String, Long>> result = keyedEdits
                    .timeWindow(Time.seconds(5)) // 每 5 秒汇总一次结果
                    .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> createAccumulator() {
                            return new Tuple2<>("", 1L);
                        }

                        /*
                         * 分区内的计算
                         * */
                        @Override
                        public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                            accumulator.f0 = value.getUser();
                            accumulator.f1 = accumulator.f1 + value.getByteDiff();
                            return accumulator;
                        }

                        /*
                         * 最终的结果
                         * */
                        @Override
                        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                            return accumulator;
                        }

                        /*
                         * 分区间合并
                         * */
                        @Override
                        public Tuple2<String, Long> merge(Tuple2<String, Long> acc2, Tuple2<String, Long> acc1) {
                            return new Tuple2<>(acc2.f0, acc2.f1 + acc1.f1);
                        }
                    });

//            result.print();

            /*
             * 将结果写入kafka
             * */
            result
                    .map(new MapFunction<Tuple2<String, Long>, String>() {
                        @Override
                        public String map(Tuple2<String, Long> value) throws Exception {
                            return value.toString();
                        }
                    })
                    .addSink(new FlinkKafkaProducer010<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));

            see.execute();
        } catch (Exception e) {
            LOGGER.error("flink 执行执行失败......\n");
            e.printStackTrace();
        }

    }
}
