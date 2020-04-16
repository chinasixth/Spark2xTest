package com.qinglianyun.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class PhoenixJDBC {
    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;

    public static void main(String[] args) {
        accessPhoenix();
    }

    /**
     * 通过jdbc访问phoenix
     */
    private static void accessPhoenix() {

        try {
            Configuration conf = HBaseConfiguration.create();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hbase"));
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:ambari-test1,ambari-test2,ambari-test3:2181");
            String sql = "select * from P";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                int va = rs.getInt(2);
                String name = rs.getString(3);
                System.out.println("id: " + id + "\tva: " + va + "\tname: " + name);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
