import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 14:53 2018/12/4
 * @
 */
public class C3P0Test {
    public static void main(String[] args) throws SQLException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource("qlcloud_c3p0");

        Connection conn;
        PreparedStatement ps;
        ResultSet rs = null;

        conn = dataSource.getConnection();
        // 批量操作关闭自动提交
//        String sql = "insert into person(name) values ('langer')";
        String sql = "update `person` set `age` = 21 where name = \"langer\"";
        ps = conn.prepareStatement(sql);

//        ps.execute();
        ps.executeUpdate();

        if (rs != null) {
            rs.close();
        }
        if (ps != null) {
            ps.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
