package com.qinglianyun.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class AccessHDFS {
    private static FileSystem fs = null;
    private static InputStream open = null;

    public static void main(String[] args) {
        accessHDFS();
    }

    public static void accessHDFS() {
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(URI.create("hdfs://139.217.102.185:8020"), conf);

            Path workingDirectory = fs.getWorkingDirectory();
            System.out.println(workingDirectory.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
