package com.qinglianyun.hive;

import java.sql.*;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class HiveJDBC {

    public static void main(String[] args) {
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://tineye-test2:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2", "hive", "");
            String hql = "select * from sys.columns_v2 limit 10";
            pstmt = conn.prepareStatement(hql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.toString());
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
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
