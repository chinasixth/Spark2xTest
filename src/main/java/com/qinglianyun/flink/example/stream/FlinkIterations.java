package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class FlinkIterations {
    private static final Logger LOGGER = LoggerFactory.getLogger("FlinkIterations");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
        * 示例：将一系列数值一直减一，直到为0
        * 这里面有些数据需要进入下一次迭代，有的数据满足为0的条件，不需要进入迭代
        * closeWith用来判断哪些数据不用进入下次迭代
        * */
        DataStreamSource<Long> someItegers = env.generateSequence(0, 1000);

        IterativeStream<Long> iteration = someItegers.iterate();

        SingleOutputStreamOperator<Long> minusone = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });

        SingleOutputStreamOperator<Long> stillGreaterThanZero = minusone.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        SingleOutputStreamOperator<Long> lessThanZero = minusone.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            LOGGER.error("flink application execute failure......" + e);
        }
    }
}
