package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class KafkaStateWordCount {
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaStateWordCount");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
//        env.setParallelism(1);

//        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
//                .keyBy(0)
//                .flatMap(new CountWindowAverage())
//                .print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.180:6667,192.168.1.181:6667,192.168.1.182:6667");
        properties.setProperty("group.id", "group_test");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);

        DataStreamSource<String> source = env.addSource(consumer010);

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        /*
         * 类似于Spark streaming中的reduceByKey，可以实现全量数据的wordcount
         * */
        words.keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("value1=" + value1.toString() + " value2=" + value2.toString());
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

//        SingleOutputStreamOperator<Tuple2<String, Long>> wordcount = words.keyBy(0)
//                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>>() {
//                    private transient MapState<String, Long> wc;
//
//                    /*
//                    * 设置key的有效期
//                    * TTL 的更新策略（默认是 OnCreateAndWrite）：
//                    * StateTtlConfig.UpdateType.OnCreateAndWrite - 仅在创建和写入时更新
//                    * StateTtlConfig.UpdateType.OnReadAndWrite - 读取时也更新
//                    *
//                    * 数据在过期但还未被清理时的可见性配置如下（默认为 NeverReturnExpired):
//                    * StateTtlConfig.StateVisibility.NeverReturnExpired - 不返回过期数据
//                    * StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp - 会返回过期但未清理的数据
//                    *
//                    * NeverReturnExpired 情况下，过期数据就像不存在一样，不管是否被物理删除。这对于不能访问过期数据的场景下非常有用，比如敏感数据。 ReturnExpiredIfNotCleanedUp 在数据被物理删除前都会返回。
//                    * */
//                    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.seconds(1))
//                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
////                            .setUpdateType() // 从快照恢复时，会删除过期数据
//                            .build();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("wordcount", Types.STRING, Types.LONG);
//                        wc = getRuntimeContext().getMapState(descriptor);
//                    }
//
//                    @Override
//                    public Tuple2<String, Long> map(Tuple2<String, Integer> value) throws Exception {
//                        if (wc.contains(value.f0)) {
//                            wc.put(value.f0, wc.get(value.f0) + 1L);
//                        } else {
//                            wc.put(value.f0, 1L);
//                        }
//
//                        return new Tuple2<>(value.f0, wc.get(value.f0));
//                    }
//                }).keyBy(0)
//                .max(1);

//        wordcount.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Double>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / (double) currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}

