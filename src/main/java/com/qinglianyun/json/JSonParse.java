package com.qinglianyun.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class JSonParse {
    private static final Logger LOGGER = LoggerFactory.getLogger("JSonParse");

    public static void main(String[] args) {
        /*
         * 初始化对象
         * */
        ObjectMapper mapper = new ObjectMapper();

        try {
            /*
             * 打开文件，读取文件内容，根据json文件的内容指定序列化类
             * 如果是{}，选择ObjectNode, readValue
             * 如果是[{},{}]选择ArrayNode， readTree
             *
             * */
            JsonNode rootNode = mapper.readValue(new File("src/main/data/flow/test.json"), JsonNode.class);

            int type = rootNode.path("type").asInt();
            String mac = rootNode.path("mac").asText();
            String pid = rootNode.path("pid").asText(); // 产品id
            int tt = rootNode.path("tt").asInt();
            int st = rootNode.path("st").asInt(); // 开始时间

            JsonNode data = rootNode.path("data");
            HashMap<Integer, String> hHM = new HashMap<>();
            HashMap<Integer, String> dHM = new HashMap<>();

            File file = new File("src/main/data/flow/test.csv");
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
            bw.write("tsec,packsize,mac,sip,dip,sport,dport,msec,protocol,len,flag,syn,syn+ack");
            bw.newLine();

            if (data instanceof ArrayNode) {
                for (JsonNode da : data) {
                    int p = da.path("p").asInt();

                    JsonNode h = da.withArray("h");
                    Iterator<JsonNode> hit = h.iterator();
                    int i = 0;
                    while (hit.hasNext()) {
                        JsonNode next = hit.next();
                        hHM.put(i, String.valueOf(next.asLong()));
                        i++;
                    }

                    JsonNode d = da.withArray("d");
                    Iterator<JsonNode> dit = d.iterator();
                    while (dit.hasNext()) {
                        JsonNode next = dit.next();
                        Iterator<JsonNode> nnext = next.iterator();

                        int j = 0;
                        while (nnext.hasNext()) {
                            JsonNode next1 = nnext.next();
                            dHM.put(j, String.valueOf(next1.asLong()));
                            j++;
                        }
                        StringBuffer sb = new StringBuffer();
                        sb.append(dHM.getOrDefault(2, "")).append(",")
                                .append(dHM.getOrDefault(3, "")).append(",")
                                .append(mac).append(",")
                                .append(pid).append(",")
                                .append(hHM.getOrDefault(0, "")).append(",")
                                .append(hHM.getOrDefault(2, "")).append(",")
                                .append(hHM.get(1)).append(",")
                                .append(hHM.get(3)).append(",")
                                .append(p).append(",")
                                .append(dHM.getOrDefault(1, "")).append(",")
                                .append(dHM.getOrDefault(0, "")).append(",")
                                .append(dHM.getOrDefault(4, "")).append(",")
                                .append(dHM.getOrDefault(5, ""));
                        String toString = sb.toString();

                        bw.write(toString);
                        bw.newLine();

                        dHM.clear();
                    }
                    hHM.clear();
                }
            }

            bw.close();


//            System.out.println("type: " + type + "\n" +
//                    "mac: " + mac + "\n" +
//                    "pid: " + pid + "\n" +
//                    "tt: " + tt + "\n" +
//                    "st: " + st + "\n" +
//                    "");
        } catch (IOException e) {
            LOGGER.error("解析Json主类异常", e);
        }
    }
}
