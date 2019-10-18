package com.qinglianyun.flink.example.stream;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class CustomWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        return new Watermark(Long.parseLong(lastElement.split(",")[0]));
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        return Long.parseLong(element.split(",")[0]);
    }
}
