package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @ flink支持的数据类型包括：Tuple，POJO，基本数据类型，Hadoop的Writable接口类型等多种类型
 * 其中POJO可以自己选择序列化的方式
 */
public class FlinkDataTypes {
    private static final Logger LOGGER = LoggerFactory.getLogger("FlinkDataTypes");


    public static void main(String[] args) {
        StreamExecutionEnvironment see = null;


        try {
            see = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<Tuple2<String, Integer>> wordcounts = see.fromElements(
                    new Tuple2<String, Integer>("hello", 1),
                    new Tuple2<String, Integer>("world", 2)
            );

            SingleOutputStreamOperator<Integer> mapped = wordcounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                @Override
                public Integer map(Tuple2<String, Integer> value) throws Exception {
                    return value.f1;
                }
            });
            mapped.print();

            KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordcounts.keyBy(0);
            keyed.print();
//            KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordcounts.keyBy("f0");

//            execute.getAccumulatorResult(); // 返回累加器的结果

            /*
             * Java lambda 表达式
             * */
            see.fromElements(1, 2, 3)
                    .map(i -> i * i)
                    .print();

            DataStreamSource<Integer> input = see.fromElements(1, 2, 3);

            input.flatMap((Integer number, Collector<String> out) -> {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < number; i++) {
                    builder.append("a");
                    out.collect(builder.toString());
                }
            }).returns(Types.STRING) // 如果不指定类型将会报错
                    .print();


            see.execute();
        } catch (Exception e) {
            LOGGER.error("flink execute failure......");
        }
    }
}
