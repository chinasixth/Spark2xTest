import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class JsonTest {

    public static void main(String[] args) {
        String json = "{\"beforeImages\": null, \"afterImages\": [{\"precision\": 8, \"value\": \"1052670\"}, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"æ ¸æ¡\\u0083ç¼\\u0096ç¨\\u008B é\\u009B¶å\\u009Fºç¡\\u0080ç¼\\u0096ç¨\\u008Bè¯¾\"}}, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"11æ\\u009C\\u008815æ\\u0097¥ã\\u0083»æ¯\\u008Få\\u0091¨äº\\u0094æ\\u0099\\u009A7ç\\u0082¹å¼\\u0080è¯¾ã\\u0083»ä¸\\u0080å\\u0091¨ä¸\\u0080è¯¾\"}}, null, {\"precision\": 8, \"value\": \"2\"}, {\"precision\": 8, \"value\": \"2467307\"}, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"17854909603\"}}, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"o-42V5LH6mcuQUcrkHXCA2hdHu1o\"}}, null, {\"precision\": 4, \"value\": \"990\"}, null, null, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"10016092\"}}, null, {\"type\": \"ENUM\", \"value\": \"UNPAID\"}, null, {\"precision\": 8, \"value\": \"130\"}, {\"precision\": 8, \"value\": \"2\"}, {\"timestamp\": 1572516770, \"millis\": 0}, {\"timestamp\": 1572516770, \"millis\": 0}, {\"precision\": 8, \"value\": \"60020\"}, {\"precision\": 8, \"value\": \"0\"}, null, {\"precision\": 4, \"value\": \"0\"}, null, null, {\"precision\": 8, \"value\": \"8\"}, {\"precision\": 4, \"value\": \"990\"}, {\"precision\": 4, \"value\": \"0\"}, {\"charset\": \"utf8mb4\", \"value\": {\"bytes\": \"\"}}, {\"type\": \"ENUM\", \"value\": \"L1\"}, null, {\"precision\": 4, \"value\": \"0\"}, {\"precision\": 4, \"value\": \"4\"}, {\"type\": \"ENUM\", \"value\": \"PAY\"}]}";
        JSONObject object = JSON.parseObject(json);
        JSONArray afterImages = object.getJSONArray("afterImages");

        for (Object afterImage : afterImages) {
            if (afterImage != null) {
                JSONObject jsonObject = JSON.parseObject(afterImage.toString());
                if (jsonObject.containsKey("precision") || jsonObject.containsKey("type")) {
                    String value = jsonObject.getString("value");
                    System.out.println(value);
                } else if (jsonObject.containsKey("charset")) {
                    JSONObject valueObject = jsonObject.getJSONObject("value");
                    String value = valueObject.getString("bytes");
                    System.out.println(value);
                }
            }
        }

        try {
            System.out.println(new String(Hex.decodeHex("\\x7F\\xFF\\xFE\\x91\\x98\\xCA\\xFF".toCharArray()), "utf-8"));
        } catch (DecoderException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
