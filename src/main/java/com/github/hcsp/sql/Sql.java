
package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

        @Override
        public String toString() {
            return "User{" + "id=" + id + ", name='" + name + '\'' + ", tel='" + tel + '\'' + ", address='" + address + '\'' + '}';
        }
    }

    /**
     * 题目1：
     * 查询有多少所有用户曾经买过指定的商品
     *
     * @param databaseConnection 数据库链接
     * @param goodsId            指定的商品ID
     * @return 有多少用户买过这个商品
     * @throws SQLException 数据库异常
     */
// 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        String sql = "select count(USER_ID)\n" +
                "from (\n" +
                "         select USER_id\n" +
                "         from \"ORDER\"\n" +
                "         where GOODS_ID = ?\n" +
                "         group by USER_ID\n" +
                "     )";
        PreparedStatement preparedStatement = databaseConnection.prepareStatement(sql);
        preparedStatement.setInt(1, goodsId);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param databaseConnection 数据库链接
     * @param pageNum            第几页，从1开始
     * @param pageSize           每页有多少个元素
     * @return 指定页中的用户
     * @throws SQLException 数据库异常
     */
// 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        List<User> userList = new ArrayList<>();
        String sql = "select id, name, tel, address from USER order by ID desc limit ? offset ?";
        PreparedStatement preparedStatement = databaseConnection.prepareStatement(sql);

        preparedStatement.setInt(1, pageSize);
        preparedStatement.setInt(2, (pageNum - 1) * pageSize);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            User user = new User();
            user.id = resultSet.getInt(1);
            user.name = resultSet.getString(2);
            user.tel = resultSet.getString(3);
            user.address = resultSet.getString(4);
            userList.add(user);
        }
        return userList;
    }

    // 商品及其营收
    public static class GoodsAndGmv {
        Integer goodsId; // 商品ID
        String goodsName; // 商品名
        BigDecimal gmv; // 商品的所有销售额

        @Override
        public String toString() {
            return "GoodsAndGmv{" + "goodsId=" + goodsId + ", goodsName='" + goodsName + '\'' + ", gmv=" + gmv + '}';
        }
    }

    /**
     * 题目3：
     * 查询所有的商品及其销售额，按照销售额从大到小排序
     *
     * @param databaseConnection 数据库链接
     * @return 返回商品销售额列表
     * @throws SQLException 数据库异常
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
        List<GoodsAndGmv> list = new ArrayList<>();
        String sql = "select GOODS.id, GOODS.NAME, GOODS_GMV.GMV\n" +
                "from GOODS join\n" +
                "(select GOODS_ID, sum(GOODS_NUM * GOODS_PRICE) as GMV from \"ORDER\" group by GOODS_ID) GOODS_GMV\n" +
                "on GOODS.ID = GOODS_GMV.GOODS_ID\n" +
                "order by GOODS_GMV.GMV desc";
        PreparedStatement preparedStatement = databaseConnection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            GoodsAndGmv goodsAndGmv = new GoodsAndGmv();
            goodsAndGmv.goodsId = resultSet.getInt(1);
            goodsAndGmv.goodsName = resultSet.getString(2);
            goodsAndGmv.gmv = resultSet.getBigDecimal(3);
            list.add(goodsAndGmv);
        }
        return list;
    }


    // 订单详细信息
    public static class Order {
        Integer id; // 订单ID
        String userName; // 用户名
        String goodsName; // 商品名
        BigDecimal totalPrice; // 订单总金额

        @Override
        public String toString() {
            return "Order{" + "id=" + id + ", userName='" + userName + '\'' + ", goodsName='" + goodsName + '\'' + ", totalPrice=" + totalPrice + '}';
        }
    }

    /**
     * 题目4：
     * 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     *
     * @param databaseConnection 数据库链接
     * @return 返回订单列表
     * @throws SQLException 数据库异常
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
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        List<Order> orderList = new ArrayList<>();
        String sql = "select \"ORDER\".id as ORDER_ID,\n" +
                "       USER.NAME  as USER_NAME,\n" +
                "       GOODS.NAME as GOODS_NAME,\n" +
                "       \"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "          join GOODS on \"ORDER\".GOODS_ID = GOODS.ID\n" +
                "          join USER on \"ORDER\".USER_ID = USER.ID\n";
        return getOrders(databaseConnection, orderList, sql);
    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     *
     * @param databaseConnection 数据库链接
     * @return 返回订单列表
     * @throws SQLException 数据库异常
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
        List<Order> orderList = new ArrayList<>();
        String sql = "select \"ORDER\".id as ORDER_ID,\n" +
                "       USER.NAME  as USER_NAME,\n" +
                "       GOODS.NAME as GOODS_NAME,\n" +
                "       \"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "         left join GOODS on \"ORDER\".GOODS_ID = GOODS.ID\n" +
                "         left join USER on \"ORDER\".USER_ID = USER.ID\n";
        return getOrders(databaseConnection, orderList, sql);
    }

    private static List<Order> getOrders(Connection databaseConnection, List<Order> orderList, String sql) throws SQLException {
        PreparedStatement preparedStatement = databaseConnection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Order order = new Order();
            order.id = resultSet.getInt(1);
            order.userName = resultSet.getString(2);
            order.goodsName = resultSet.getString(3);
            order.totalPrice = resultSet.getBigDecimal(4);
            orderList.add(order);
        }
        return orderList;
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
