import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class PropertyUtilTest {

    private static FileSystem fs = null;
    private static FSDataInputStream open = null;

    public static void main(String[] args) {
        test();
    }

    public static void test() {

        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(URI.create("hdfs://192.168.1.180:8020"), conf);

            open = fs.open(new Path("/user/spark/jdbc.properties"));
            Properties prop = new Properties();
            prop.load(open);
            String url = prop.getProperty("url");
            String user = prop.getProperty("username");
            String password = prop.getProperty("password");

            System.out.println("url=" + url + "\nuser=" + user + "\npassword=" + password);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (open != null) {
                try {
                    open.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
