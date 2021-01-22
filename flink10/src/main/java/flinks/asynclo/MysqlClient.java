package flinks.asynclo;



import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author lj
 * @createDate 2019/12/27 16:25
 **/
public class MysqlClient {
    private static String jdbcUrl = "jdbc:mysql://192.168.229.128:3306?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "root";
    private static String password = "123456";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            ps = conn.prepareStatement("select phone from async.async_test where id = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * execute query
     *
     * @param user
     * @return
     */
    public AsyncUser query1(AsyncUser user) {

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String phone = "0000";
        try {
            ps.setString(1, user.getId());
            ResultSet rs = ps.executeQuery();
            if (!rs.isClosed() && rs.next()) {
                phone = rs.getString(1);
            }
            System.out.println("execute query : " + user.getId() + "-2-" + "phone : " + phone + "-" + System.currentTimeMillis());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        user.setPhone(phone);
        return user;

    }

    // 测试代码
//    public static void main(String[] args) {
//        MysqlClient mysqlClient = new MysqlClient();
//
//        AsyncUser asyncUser = new AsyncUser();
//        asyncUser.setId("526");
//        long start = System.currentTimeMillis();
//        asyncUser = mysqlClient.query1(asyncUser);
//
//        System.out.println("end : " + (System.currentTimeMillis() - start));
//        System.out.println(asyncUser.toString());
//    }
}
