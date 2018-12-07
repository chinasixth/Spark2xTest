import com.qinglianyun.jdbc.JDBCUtil;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 17:13 2018/12/4
 * @
 */
public class JDBCUtilTest {
    public static void main(String[] args) {
        JDBCUtil jdbcUtil = JDBCUtil.getInstance();

        String sql = "insert into person values(?, ?)";
        Object[] params = new Object[]{
                "jiajia", 19
        };


        jdbcUtil.execute(sql, params);
    }
}
