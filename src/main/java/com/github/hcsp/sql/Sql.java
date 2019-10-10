
package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class Sql {
// 用户表：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
// | 2  | lisi     | tel2 | shanghai |
// +----+----------+------+----------+
// | 3  | wangwu   | tel3 | shanghai |
// +----+----------+------+----------+
// | 4  | zhangsan | tel4 | shenzhen |
// +----+----------+------+----------+
// 商品表：
// +----+--------+-------+
// | ID | NAME   | PRICE |
// +----+--------+-------+
// | 1  | goods1 | 10    |
// +----+--------+-------+
// | 2  | goods2 | 20    |
// +----+--------+-------+
// | 3  | goods3 | 30    |
// +----+--------+-------+
// | 4  | goods4 | 40    |
// +----+--------+-------+
// | 5  | goods5 | 50    |
// +----+--------+-------+
// 订单表：
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | ID(订单ID) | USER_ID(用户ID) | GOODS_ID(商品ID) | GOODS_NUM(商品数量) | GOODS_PRICE(下单时的商品单价)        |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 1          | 1               | 1                | 5                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 2          | 2               | 1                | 1                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 3          | 2               | 1                | 2                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 4          | 4               | 2                | 4                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 5          | 4               | 2                | 100                 | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 6          | 4               | 3                | 1                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 7          | 5               | 4                | 1                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 8          | 5               | 6                | 1                   | 60                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+

    // 用户信息
    public static class User {
        Integer id;
        String name;
        String tel;
        String address;

        public static User createUser(Integer id, String name, String tel, String address) {
            User u = new User();
            u.id = id;
            u.name = name;
            u.tel = tel;
            u.address = address;
            return u;
        }

        @Override
        public String toString() {
            return "User{" + "id=" + id + ", name='" + name + '\'' + ", tel='" + tel + '\'' + ", address='" + address + '\'' + '}';
        }
    }

    /**
     * 题目1：
     * 查询有多少所有用户曾经买过指定的商品
     *
     * @param goodsId 指定的商品ID
     * @param databaseConnection connect
     * @throws  SQLException 数据库错误
     * @return 有多少用户买过这个商品
     */
// 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("SELECT COUNT(DISTINCT USER_ID) FROM `ORDER` WHERE GOODS_ID = ?;")) {
            statement.setInt(1, goodsId);

            ResultSet result = statement.executeQuery();

            while (result.next()) {
                return result.getInt(1);
            }
        }
        return 0;
    }

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param pageNum  第几页，从1开始
     * @param pageSize 每页有多少个元素
     * @param databaseConnection connect
     * @throws  SQLException 数据库错误
     * @return 指定页中的用户
     */
// 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        ArrayList<User> list = new ArrayList<>();

        try (PreparedStatement statement = databaseConnection.prepareStatement("SELECT ID, NAME, TEL, ADDRESS FROM `USER` ORDER BY ID DESC LIMIT ? OFFSET ?;")) {
            int offset = pageNum >= 1 ? pageSize * (pageNum - 1) : 0;

            statement.setInt(1, pageSize);
            statement.setInt(2, offset);

            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt(1);
                String name = result.getString(2);
                String tel = result.getString(3);
                String address = result.getString(4);

                list.add(User.createUser(id, name, tel, address));
            }
        }
        return list.size() > 0 ? list : null;
    }

    // 商品及其营收
    public static class GoodsAndGmv {
        Integer goodsId; // 商品ID
        String goodsName; // 商品名
        BigDecimal gmv; // 商品的所有销售额

        public static GoodsAndGmv createGoodsAndGmv(int goodsId, String goodsName, BigDecimal gmv) {
            GoodsAndGmv g = new GoodsAndGmv();
            g.goodsId = goodsId;
            g.goodsName = goodsName;
            g.gmv = gmv;
            return g;
        }

        @Override
        public String toString() {
            return "GoodsAndGmv{" + "goodsId=" + goodsId + ", goodsName='" + goodsName + '\'' + ", gmv=" + gmv + '}';
        }
    }

    /**
     * 题目3：
     * 查询所有的商品及其销售额，按照销售额从大到小排序
     * @param databaseConnection connect
     * @throws  SQLException 数据库错误
     * @return List<GoodsAndGmv>
     */
// 预期的结果应该如图所示
//  +----+--------+------+
//  | ID | NAME   | GMV  |
//  +----+--------+------+
//  | 2  | goods2 | 2080 |
//  +----+--------+------+
//  | 1  | goods1 | 80   |
//  +----+--------+------+
//  | 4  | goods4 | 20   |
//  +----+--------+------+
//  | 3  | goods3 | 20   |
//  +----+--------+------+
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        ArrayList<GoodsAndGmv> list = new ArrayList<>();

        String sql = "SELECT `GOODS`.ID as ID, `GOODS`.NAME as NAME, SUM(`ORDER`.GOODS_NUM) * `ORDER`.GOODS_PRICE as GMV FROM `GOODS` " +
                "JOIN `ORDER` " +
                "ON `ORDER`.GOODS_ID = `GOODS`.ID GROUP BY `GOODS`.ID ORDER BY GMV DESC;";

        try (PreparedStatement statement = databaseConnection.prepareStatement(sql)) {

            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt(1);
                String name = result.getString(2);
                BigDecimal gmv = result.getBigDecimal(3);

                list.add(GoodsAndGmv.createGoodsAndGmv(id, name, gmv));
            }
        }
        return list.size() > 0 ? list : null;
    }


    // 订单详细信息
    public static class Order {
        Integer id; // 订单ID
        String userName; // 用户名
        String goodsName; // 商品名
        BigDecimal totalPrice; // 订单总金额

        public static Order createOrder(int id, String userName, String goodsName, BigDecimal totalPrice) {
            Order order = new Order();
            order.id = id;
            order.userName = userName;
            order.goodsName = goodsName;
            order.totalPrice = totalPrice;
            return order;
        }

        @Override
        public String toString() {
            return "Order{" + "id=" + id + ", userName='" + userName + '\'' + ", goodsName='" + goodsName + '\'' + ", totalPrice=" + totalPrice + '}';
        }
    }

    /**
     * 题目4：
     * 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     * @param databaseConnection connect
     * @throws  SQLException 数据库错误
     * @return List<Order>
     */
// 预期的结果为：
// +----------+-----------+------------+-------------+
// | ORDER_ID | USER_NAME | GOODS_NAME | TOTAL_PRICE |
// +----------+-----------+------------+-------------+
// | 1        | zhangsan  | goods1     | 50          |
// +----------+-----------+------------+-------------+
// | 2        | lisi      | goods1     | 10          |
// +----------+-----------+------------+-------------+
// | 3        | lisi      | goods1     | 20          |
// +----------+-----------+------------+-------------+
// | 4        | zhangsan  | goods2     | 80          |
// +----------+-----------+------------+-------------+
// | 5        | zhangsan  | goods2     | 2000        |
// +----------+-----------+------------+-------------+
// | 6        | zhangsan  | goods3     | 20          |
// +----------+-----------+------------+-------------+
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        ArrayList<Order> list = new ArrayList<>();

        String sql = "SELECT `ORDER`.ID as ORDER_ID, `USER`.NAME as USER_NAME, `GOODS`.NAME as GOODS_NAME, `ORDER`.GOODS_PRICE * `ORDER`.GOODS_NUM as TOTAL_PRICE FROM `ORDER` " +
                "INNER JOIN `USER` " +
                "ON `USER`.ID = `ORDER`.USER_ID " +
                "INNER JOIN `GOODS`" +
                "ON `GOODS`.ID = `ORDER`.GOODS_ID;";

        try (PreparedStatement statement = databaseConnection.prepareStatement(sql)) {

            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt(1);
                String userName = result.getString(2);
                String goodsName = result.getString(3);
                BigDecimal totalPrice = result.getBigDecimal(4);

                list.add(Order.createOrder(id, userName, goodsName, totalPrice));
            }
        }
        return list.size() > 0 ? list : null;
    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     * @param databaseConnection connect
     * @throws  SQLException 数据库错误
     * @return List<Order>
     */
// 预期的结果为：
// +----------+-----------+------------+-------------+
// | ORDER_ID | USER_NAME | GOODS_NAME | TOTAL_PRICE |
// +----------+-----------+------------+-------------+
// | 1        | zhangsan  | goods1     | 50          |
// +----------+-----------+------------+-------------+
// | 2        | lisi      | goods1     | 10          |
// +----------+-----------+------------+-------------+
// | 3        | wangwu    | goods1     | 20          |
// +----------+-----------+------------+-------------+
// | 4        | zhangsan  | goods2     | 80          |
// +----------+-----------+------------+-------------+
// | 5        | zhangsan  | goods2     | 2000        |
// +----------+-----------+------------+-------------+
// | 6        | zhangsan  | goods3     | 20          |
// +----------+-----------+------------+-------------+
// | 7        | NULL      | goods4     | 20          |
// +----------+-----------+------------+-------------+
// | 8        | NULL      | NULL       | 60          |
// +----------+-----------+------------+-------------+
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        ArrayList<Order> list = new ArrayList<>();

        String sql = "SELECT `ORDER`.ID as ORDER_ID, `USER`.NAME as USER_NAME, `GOODS`.NAME as GOODS_NAME, `ORDER`.GOODS_PRICE * `ORDER`.GOODS_NUM as TOTAL_PRICE FROM `ORDER` " +
                "LEFT JOIN `USER` " +
                "ON `USER`.ID = `ORDER`.USER_ID " +
                "LEFT JOIN `GOODS`" +
                "ON `GOODS`.ID = `ORDER`.GOODS_ID ORDER BY `ORDER`.ID;";

        try (PreparedStatement statement = databaseConnection.prepareStatement(sql)) {

            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt(1);
                String userName = result.getString(2);
                String goodsName = result.getString(3);
                BigDecimal totalPrice = result.getBigDecimal(4);

                list.add(Order.createOrder(id, userName, goodsName, totalPrice));
            }
        }
        return list.size() > 0 ? list : null;
    }

    // 注意，运行这个方法之前，请先运行mvn initialize把测试数据灌入数据库
    public static void main(String[] args) throws SQLException {
        File projectDir = new File(System.getProperty("basedir", System.getProperty("user.dir")));
        String jdbcUrl = "jdbc:h2:file:" + new File(projectDir, "target/test").getAbsolutePath();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "root", "Jxi1Oxc92qSj")) {
            System.out.println(countUsersWhoHaveBoughtGoods(connection, 1));
            System.out.println(getUsersByPageOrderedByIdDesc(connection, 2, 3));
            System.out.println(getGoodsAndGmv(connection));
            System.out.println(getInnerJoinOrders(connection));
            System.out.println(getLeftJoinOrders(connection));
        }
    }

}
