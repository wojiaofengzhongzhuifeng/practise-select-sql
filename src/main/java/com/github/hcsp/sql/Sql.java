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
// 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+

    /**
     * 题目1：
     * 查询有多少所有用户曾经买过指定的商品
     *
     * @param databaseConnection 获取数据库连接
     * @param goodsId            指定的商品IDd
     * @return 有多少用户买过这个商品
     * @throws SQLException SQL异常
     */
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        int result = -1;
        PreparedStatement ps = getPreparedStatementBySQL(databaseConnection, "SELECT count(distinct p.user_id) as total FROM `ORDER` p WHERE p.goods_id = ? ;");
        ps.setInt(1, goodsId);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getInt(1);
        }
        // 关闭连接
        closePreparedStatement(ps);
        return result;
    }


    /**
     * 关闭PreparedStatement对象
     *
     * @param ps 预处理对象
     * @throws SQLException SQL异常
     */
    private static void closePreparedStatement(PreparedStatement ps) throws SQLException {
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 根据sql语句获取PreparedStatement对象
     *
     * @param databaseConnection 获取数据库连接
     * @param sql                sql语句
     * @return
     * @throws SQLException SQL异常
     */
    private static PreparedStatement getPreparedStatementBySQL(Connection databaseConnection, String sql) throws SQLException {
        return databaseConnection.prepareStatement(sql);
    }


// 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param databaseConnection 获取数据库连接
     * @param pageNum            第几页，从1开始
     * @param pageSize           每页有多少个元素
     * @return 指定页中的用户
     * @throws SQLException SQL异常
     */
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        List<User> result = new ArrayList<>();

        PreparedStatement ps = getPreparedStatementBySQL(databaseConnection, "select * from  `user` order by id desc limit ? , ? ;");
        ps.setInt(1, (pageNum - 1) * pageSize);
        ps.setInt(2, pageSize);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String name = resultSet.getString(2);
            String tel = resultSet.getString(3);
            String address = resultSet.getString(4);
            result.add(new User(id, name, tel, address));
        }
        closePreparedStatement(ps);
        return result;
    }
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

    /**
     * 题目3：
     * 查询所有的商品及其销售额，按照销售额从大到小排序
     *
     * @param databaseConnection 获取数据库连接
     * @return 结果数据
     * @throws SQLException SQL异常
     */
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        List<GoodsAndGmv> result = new ArrayList<>();

        PreparedStatement ps = getPreparedStatementBySQL(databaseConnection, "select g.id,g.name,tmp.GMV from (\n" +
                "    select  o.goods_id,sum(o.goods_num * o.goods_price) as GMV\n" +
                "    from `order` o\n" +
                "    group by  o.goods_id\n" +
                "    order by GMV desc\n" +
                "    ) tmp\n" +
                "inner join goods g on tmp.goods_id = g.id\n" +
                "order by GMV desc");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String name = resultSet.getString(2);
            BigDecimal gmv = resultSet.getBigDecimal(3);
            result.add(new GoodsAndGmv(id, name, gmv));
        }
        closePreparedStatement(ps);
        return result;
    }


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

    /**
     * 题目4：
     * 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     *
     * @param databaseConnection 获取数据库连接
     * @return 结果数据
     * @throws SQLException SQL异常
     */
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        List<Order> result = new ArrayList<>();

        PreparedStatement ps = getPreparedStatementBySQL(databaseConnection, "select\n" +
                "    o.id order_id,\n" +
                "    u.name user_name,\n" +
                "    g.name goods_name,\n" +
                "    o.goods_num * o.goods_price total_price\n" +
                "from\n" +
                "   `order` o\n" +
                "inner join goods g on o.goods_id = g.id\n" +
                "inner join `user` u on o.user_id = u.id");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int orderId = resultSet.getInt(1);
            String name = resultSet.getString(2);
            String goodsName = resultSet.getString(3);
            BigDecimal total = resultSet.getBigDecimal(4);
            result.add(new Order(orderId, name, goodsName, total));
        }
        closePreparedStatement(ps);
        return result;
    }


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

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     *
     * @param databaseConnection 获取数据库连接
     * @return 结果数据
     * @throws SQLException SQL异常
     */
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        List<Order> result = new ArrayList<>();
        PreparedStatement ps = getPreparedStatementBySQL(databaseConnection, "select\n" +
                "    o.id order_id,\n" +
                "    u.name user_name,\n" +
                "    g.name goods_name,\n" +
                "    o.goods_num * o.goods_price total_price\n" +
                "from\n" +
                "    `order` o\n" +
                "left join `user` u on o.user_id = u.id\n" +
                "left join goods g on o.goods_id = g.id");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int orderId = resultSet.getInt(1);
            String name = resultSet.getString(2);
            String goodName = resultSet.getString(3);
            BigDecimal total = resultSet.getBigDecimal(4);
            result.add(new Order(orderId, name, goodName, total));
        }
        closePreparedStatement(ps);
        return result;
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

    // 用户信息
    public static class User {
        Integer id;
        String name;
        String tel;
        String address;

        public User(Integer id, String name, String tel, String address) {
            this.id = id;
            this.name = name;
            this.tel = tel;
            this.address = address;
        }

        @Override
        public String toString() {
            return "User{" + "id=" + id + ", name='" + name + '\'' + ", tel='" + tel + '\'' + ", address='" + address + '\'' + '}';
        }
    }

    // 商品及其营收
    public static class GoodsAndGmv {
        Integer goodsId; // 商品ID
        String goodsName; // 商品名
        BigDecimal gmv; // 商品的所有销售额

        public GoodsAndGmv(Integer goodsId, String goodsName, BigDecimal gmv) {
            this.goodsId = goodsId;
            this.goodsName = goodsName;
            this.gmv = gmv;
        }

        @Override
        public String toString() {
            return "GoodsAndGmv{" + "goodsId=" + goodsId + ", goodsName='" + goodsName + '\'' + ", gmv=" + gmv + '}';
        }
    }

    // 订单详细信息
    public static class Order {
        Integer id; // 订单ID
        String userName; // 用户名
        String goodsName; // 商品名
        BigDecimal totalPrice; // 订单总金额

        public Order(Integer id, String userName, String goodsName, BigDecimal totalPrice) {
            this.id = id;
            this.userName = userName;
            this.goodsName = goodsName;
            this.totalPrice = totalPrice;
        }

        @Override
        public String toString() {
            return "Order{" + "id=" + id + ", userName='" + userName + '\'' + ", goodsName='" + goodsName + '\'' + ", totalPrice=" + totalPrice + '}';
        }
    }

}
