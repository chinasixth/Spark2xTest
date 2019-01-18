import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 11:39 2019/1/16
 * @ desc: 给定一个正则表达式，能够从字符串中匹配到多个结果，如何将这些结果全部拿到。
 */
public class AddTest {
    public static void main(String[] args) {
        // 1231231%123%2#2#采了香水去屑止痒洗发水750ml+沐浴露^CNY86.80^1#香港满芝堂 草本去螨洗面奶女男氨基^CNY49.00^1|；CNIdfsdf：z320420398098；CNIdfs234234：z23423423
        String data = "CNRemark:2018111035885106283352720200207; CNTransFee:0.00; CNAuthCode:AC00; " +
                "CNAuthInfo:CON060200012018061915439374; CNTranChnlType:07; CNSttlmDt:2018-11-10; " +
                "CNPayeeAccTyp:PT08; CNOrdrId:20181110131750211200988938183; CNOrdrDesc:1|1|" +
                "京东自营%22792279%01%11%91310114086194327Z%7399%3#3#葡萄糖Q淘逻辑派对儿童思维逻辑训练早^CNY149.00^1" +
                "#葡萄Q淘逻辑派对儿童思维逻辑训练早^CNY149.00^1#三只松鼠休闲零食特产混合什锦果蔬菜干蔬果^CNY30.00^1|; " +
                "CNInstgId:Z2014811000011; CNInstgAcctId:338961369227; CNInstgAcctIssrId:C1010411000013; CNBatchId:B201811100014;";

        Pattern pattern = Pattern.compile("(\\^CNY\\d+.\\d+)");

        Matcher mat = pattern.matcher(data);

        double total = 0.0;
        // 获得匹配到的多个内容
        while (mat.find()) {
            String group = mat.group();
            String replace = group.replace("^CNY", "");
            double jine = Double.parseDouble(replace);
            total += jine;
        }

        // 设置保留几位小数。
        DecimalFormat decimalFormat = new DecimalFormat("0.00");

        System.out.println(decimalFormat.format(total));
    }
}