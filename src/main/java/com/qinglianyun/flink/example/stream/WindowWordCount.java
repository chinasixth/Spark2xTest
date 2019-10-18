package com.qinglianyun.flink.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class WindowWordCount {
    private static final Logger LOGGER = LoggerFactory.getLogger("WindowWordCount");

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * 默认情况下，数据不会一条一条的传输，会导致不必要的网络通信，但是数据会进行缓冲
         * 缓冲的大小可以在flink配置文件中配置，此种做法可以优化吞吐量，但是，有可能会造成网络延迟
         * 当缓冲池未满时，就会一直等待满的状态，从而导致网络延迟
         * 可以设置缓冲池超时时间，默认为100毫秒
         * 如果设置为-1，将会取消超时，设置为0，会导致严重的性能下滑
         * */
        env.setBufferTimeout(100);
        /*
         * flink window 操作可以分为time-base的window和count-base的window
         * 基于时间的窗口操作需要指定时间类型，flink程序中有三种时间概念
         * event time：事件产生的事件
         * ingestion time：flink 读取 事件的事件
         * processing time：flink真正处理数据的事件
         *
         * */
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("192.168.1.181", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String str : value.split(" ")) {
                            out.collect(new Tuple2<>(str, 1));
                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(30), Time.seconds(5))  // 聚合30秒内的数据，5秒展示一次结果
                .sum(1);

        dataStream.print();


        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    /*
                    * 分配时间戳的方式有两种：
                    * 一是直接在流数据源中将时间戳分配给数据，同时可以发出水印
                    * 二是通过时间戳分配器/水印生成器
                    *
                    * 如果同时采用了这两种方式，则第二种方式会覆盖第一种方式
                    * */
                    ctx.collectWithTimestamp("", 1);
                    ctx.emitWatermark(new Watermark(1));
                }
            }

            @Override
            public void cancel() {

            }
        });
        try {
            env.execute();
        } catch (Exception e) {
            LOGGER.error("flink application execute failure......" + e);
        }
    }
}
