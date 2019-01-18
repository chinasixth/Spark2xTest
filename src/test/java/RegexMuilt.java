import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 14:51 2019/1/16
 * @ desc: 多个()匹配，且每个括号都匹配到多个内容
 */
public class RegexMuilt {
    public static void main(String[] args) {
        String data = "1231231%123%2#2#采了香水去屑止痒洗发水750ml+沐浴露^CNY86.80^1#香港满芝堂 草本去螨洗面奶女男氨基^CNY49.00^1|；CNIdfsdf：z320420398098；CNIdfs234234：z23423423";

        Pattern pattern = Pattern.compile("(\\d+)(\\w+)");

        Matcher matcher = pattern.matcher(data);

        while (matcher.find()) {
            System.out.println(matcher.group());
        }
        // 1231231
        // 123
        // 750ml
        // 86
        // 80
        // 49
        // 00
        // 320420398098
        // 234234
        // 23423423


    }
}
