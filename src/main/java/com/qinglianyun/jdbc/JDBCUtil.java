package com.qinglianyun.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 16:41 2018/12/4
 * @ 使用单例创建JDNCUtil
 * 使用c3p0连接池
 */
public class JDBCUtil {

    private static JDBCUtil instance = null;

    public ComboPooledDataSource dataSource = null;

    /*
     * 单例模式创建类的单例对象（懒汉模式）
     * */
    public static JDBCUtil getInstance() {
        if (instance == null) {
            synchronized (JDBCUtils.class) {
                if (instance == null) {
                    instance = new JDBCUtil();
                }
            }
        }
        return instance;
    }

    private JDBCUtil() {
        dataSource = new ComboPooledDataSource("qlcloud_c3p0");
    }

    /*
    * 插入一条记录，返回受影响的行数
    * */
    public int execute(String sql, Object[] params) {
        Connection conn;
        PreparedStatement ps;
        int rtn = 0;
        try {
            conn = dataSource.getConnection();
            QueryRunner queryRunner = new QueryRunner();
            rtn = queryRunner.execute(conn, sql, params);
            DbUtils.close(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rtn;
    }


}
