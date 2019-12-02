import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class CalendarTest {

    public static void main(String[] args) {
        test();
    }

    public static void test() {
        String timestamp = "2019-10-20 12:13:14";

        Timestamp ts = Timestamp.valueOf(timestamp);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(ts.getTime());

        int date = calendar.get(Calendar.YEAR);
        System.out.println("date=" + date);
    }
}
