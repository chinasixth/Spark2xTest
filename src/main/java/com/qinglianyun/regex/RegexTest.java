package com.qinglianyun.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 11:25 2019/1/7
 * @ desc:
 * 使用正则表达式，匹配出电影名字和网址
 * 109|Mystery Science Theater 3000: The Movie (1996)|19-Apr-1996||http://us.imdb.com/M/title-exact?Mystery%20Science%20Theater%203000:%20The%20Movie%20(1996)|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|1|0|0|0
 */
public class RegexTest {
    public static void main(String[] args) {
        String str = "109|Mystery Science Theater 3000: The Movie (1996)|19-Apr-1996||" +
                "http://us.imdb.com/M/title-exact?Mystery%20Science%20Theater%203000:%20The%20Movie%20(1996)" +
                "|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|1|0|0|0";

        String regexp = "^\\d*\\|([\\S* ]*)\\(\\d{4}\\S*\\|\\|([a-zA-Z]*://\\S*\\(\\d{4}\\))\\S*";

        Pattern pattern = Pattern.compile(regexp);

        Matcher matcher = pattern.matcher(str);

        if (matcher.find()) {
            int count = matcher.groupCount();

            for (int i = 0; i <= count; i++){
                String s = matcher.group(i);
                System.out.println(i + "  " + s);
            }
        }

    }
}
