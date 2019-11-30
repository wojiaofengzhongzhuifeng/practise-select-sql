package com.github.hcsp.sql;
import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
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

    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement("select count(distinct USER_ID) from \"ORDER\" where GOODS_ID=?");

        statement.setInt(1, goodsId);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }


    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        List<User> list = new ArrayList<>();

        PreparedStatement statement = databaseConnection.prepareStatement("select * from USER order by id desc limit ?,?");
        statement.setInt(1, (pageNum - 1) * pageSize);
        statement.setInt(2, pageSize);
        final ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            User user = new User();
            user.id = resultSet.getInt(1);
            user.name = resultSet.getString(2);
            user.tel = resultSet.getString(3);
            user.address = resultSet.getString(4);
            list.add(user);
        }
        return list;

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


    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement("select GOODS.ID , GOODS.NAME ,sum(\"ORDER\".GOODS_NUM*\"ORDER\".GOODS_PRICE) as GMV\n" +
                "from GOODS\n" +
                "         join \"ORDER\"\n" +
                "                   on GOODS.ID = \"ORDER\".GOODS_ID\n" +
                "group by GOODS.ID order by GMV desc ");
        ResultSet resultSet = statement.executeQuery();
        List<GoodsAndGmv> list = new ArrayList<>();
        while (resultSet.next()) {
            GoodsAndGmv gmv = new GoodsAndGmv();
            gmv.goodsId = resultSet.getInt(1);
            gmv.goodsName = resultSet.getString(2);
            gmv.gmv = resultSet.getBigDecimal(3);
            list.add(gmv);
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


    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".ID, USER.NAME, GOODS.NAME, (GOODS_NUM * GOODS_PRICE) as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "         join USER\n" +
                "              on USER_ID = USER.ID\n" +
                "         join GOODS\n" +
                "              on GOODS_ID = GOODS.ID");
        ResultSet resultSet = statement.executeQuery();
        List<Order> list = new ArrayList<>();

        while (resultSet.next()) {
            Order order = new Order();
            order.id = resultSet.getInt(1);
            order.userName = resultSet.getString(2);
            order.goodsName = resultSet.getString(3);
            order.totalPrice = resultSet.getBigDecimal(4);
            list.add(order);
        }

        return list;
    }


    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".ID, USER.NAME, GOODS.NAME, (GOODS_NUM * GOODS_PRICE) as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "         left join USER\n" +
                "                   on USER_ID = USER.ID\n" +
                "         left join GOODS\n" +
                "                   on GOODS_ID = GOODS.ID");
        ResultSet resultSet = statement.executeQuery();
        List<Order> list = new ArrayList<>();

        while (resultSet.next()) {
            Order order = new Order();
            order.id = resultSet.getInt(1);
            order.userName = resultSet.getString(2);
            order.goodsName = resultSet.getString(3);
            order.totalPrice = resultSet.getBigDecimal(4);
            list.add(order);
        }

        return list;
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
