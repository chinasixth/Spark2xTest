package com.qinglianyun.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 14:47 2019/1/17
 * @ desc: 用一个正则表达式，匹配多种子串，且每一个子串在字符串中出现多次。
 */
public class MultiCompileRegex {
    public static void main(String[] args) {
        String data = "CNRemark:2018111035885106283352720200207; CNTransFee:0.00; CNAuthCode:AC00; CNAuthInfo:CON060200012018061915439374; CNTranChnlType:07; " +
                "CNSttlmDt:2018-11-10; CNPayeeAccTyp:PT08; CNOrdrId:20181110131750211200988938183; " +
                "CNOrdrDesc:1|1|京东自营%22792279%01%11%91310114086194327Z%7399%3#3#葡萄糖Q淘逻辑派对儿童思维逻辑训练早^CNY149.00^1#葡萄Q淘逻辑派对儿童思维逻辑训练早^CNY149.00^1#三只松鼠休闲零食特产混合什锦果蔬菜干蔬果^CNY30.00^1|;" +
                " CNInstgId:Z2014811000011; CNInstgAcctId:338961369227; CNInstgAcctIssrId:C1010411000013; CNBatchId:B201811100014;";

        // 先匹配出来平台信息
        String regexPlatform = "\\d+\\|\\d+\\|([a-zA-Z\\u4e00-\\u9fa5]*)";
        Matcher matcher1 = Pattern.compile(regexPlatform).matcher(data);
        String platform = null;
        if (matcher1.find()) {
            platform = matcher1.group(1);
        }
        System.out.println(platform);

        // 匹配商品信息
        String regex = "([().0-9a-zA-Z\\u4e00-\\u9fa5]*)\\^CNY(\\d+.\\d+)\\^(\\d+).";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(data);

        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                System.out.println(matcher.group(i));
            }
        }
    }
}
