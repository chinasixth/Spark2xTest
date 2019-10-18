package com.qinglianyun.flink.example.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class FlinkWordCount {
    private static final Logger LOGGER = LoggerFactory.getLogger("FlinkWordCount");

    public static void main(String[] args) {


        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            env.getConfig().setGlobalJobParameters(params);

            // 获取数据
            DataSet<String> text;
            if (params.has("input")) {
                text = env.readTextFile(params.get("input"));
            } else {
                System.out.println("Executing WordCount example with default input data set.");
                System.out.println("Use --input to specify file input.");
                text = env.readTextFile("src/main/data/word.txt");
            }

            DataSet<Tuple2<String, Integer>> counts =
                    text.flatMap(new Tokenizer())
                            // 可以根据序号直接指定列
                            .groupBy(0)
                            .sum(1);

            if (params.has("output")) {
                counts.writeAsCsv(params.get("output"));

                // 执行程序
                env.execute("Flink WordCount Example");
            } else {
                System.out.println("Printing result to stdout. Use --output to specify output path.");
                counts.print();
            }

        } catch (Exception e) {

        }

    }

    /**
     * 自定义函数
     */
    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] split = value.split(" ");

            for (String s : split) {
                if (s.length() > 0) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }
    }
}
