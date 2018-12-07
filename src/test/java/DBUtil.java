import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 15:26 2018/12/4
 * @
 */
public class DBUtil {
    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;

    @Before
    public void instance() {
        ComboPooledDataSource dataSource = new ComboPooledDataSource("qlcloud_c3p0");
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void execute() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();

        String sql = "insert into person values ('nana', 22)";

        int execute = queryRunner.execute(conn, sql);
        System.out.println(execute);
        DbUtils.close(conn);
    }

    /*
     * 当查询结果有多个的时候，只能返回一个结果。
     * */
    @Test
    public void query() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from person";
        Object[] objects = queryRunner.query(conn, sql, new ArrayHandler());
        for (Object object : objects) {
            System.out.println(object.toString());
        }
        System.out.println(objects.length);

        DbUtils.close(conn);
    }

    /*
     * 返回所有的查询结果
     * 注意和只能返回一个结果的区别
     * */
    @Test
    public void queryAll() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from person";
        List<Object[]> objects = queryRunner.query(conn, sql, new ArrayListHandler());
        for (Object[] object : objects) {
            System.out.println(object[0].toString() + "   " + object[1].toString());
        }

        DbUtils.close(conn);
    }

    /*
     * 可以将返回的结果指定成某个对象，如此，返回的结果就是对象的集合了
     * 注意和返回所有值的区别
     * 也可以只返回一个对象
     * 注意：在对象的class中，要有getter和setter方法
     * */
    @Test
    public void queryAllObject() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from person";
        List<Person> personList = queryRunner.query(conn, sql, new BeanListHandler<>(Person.class));
        for (Person person : personList) {
            System.out.println(person.toString());
        }
    }

    @Test
    public void executeParams() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "insert into person values (?, ?)";

        Object[] params = new Object[]{"xixi", 21};
        int execute = queryRunner.execute(conn, sql, params);
        System.out.println(execute);

        DbUtils.close(conn);
    }

    @Test
    public void executeBatchParams() throws SQLException {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "insert into person values(?,?)";

        Object[][] params = new Object[][]{
                {"aa", 11},
                {"bb", 11}
        };
        int[] batch = queryRunner.batch(conn, sql, params);
        for (int i : batch) {
            System.out.println(i);
        }
        DbUtils.close(conn);
    }
}