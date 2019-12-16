
package com.github.hcsp.sql;
import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.sql.SQLException;
import java.util.List;

public class Sql {
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

    /**
     *ggggggg
     * @param databaseConnection 数据库连接
     * @param goodsId 指定的商品ID
     * @return 有多少用户买过这个商品
     * @throws SQLException 数据库连接异常
     */
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("SELECT  count(distinct USER_ID) from \"ORDER\" where GOODS_ID=?")) {
            statement.setInt(1, goodsId);

            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getInt(1);
            }
        }
        return -1;
    }

    /**
     *
     * @param databaseConnection 数据库连接
     * @param pageNum 取第几页
     * @param pageSize 每页有多少个元素
     * @return 指定页中的用户
     * @throws SQLException 数据库连接异常
     */
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("select id,name,TEL,ADDRESS from USER order by ID desc limit ?,?;")) {
            statement.setInt(1, (pageNum - 1) * pageSize);
            statement.setInt(2, pageSize);

            List<User> users = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                User user = new User(resultSet.getInt(1),
                        resultSet.getString(2),
                        resultSet.getString(3),
                        resultSet.getString(4));
                users.add(user);
            }
            return users;
        }
    }
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

    /**
     *
     * @param databaseConnection 数据库连接
     * @return 查询所有的商品及其销售额，按照销售额从大到小排序
     * @throws SQLException 数据库连接异常
     */
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("select GOODS.ID,GOODS.NAME,SUM(\"ORDER\".GOODS_NUM*\"ORDER\".GOODS_PRICE) as GMV\n" +
                "from \"ORDER\"\n" +
                "join GOODS\n" +
                "on \"ORDER\".GOODS_ID=GOODS.ID\n" +
                "group by GOODS.ID\n" +
                "order by GMV desc")) {

            List<GoodsAndGmv> goodsAndGmvs = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                GoodsAndGmv goodsAndGmv = new GoodsAndGmv(resultSet.getInt(1),
                        resultSet.getString(2),
                        resultSet.getBigDecimal(3));
                goodsAndGmvs.add(goodsAndGmv);
            }
            return goodsAndGmvs;
        }
    }
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

    /**
     *
     * @param databaseConnection 数据库连接
     * @return 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     * @throws SQLException 数据库连接异常
     */
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".ID as ORDER_ID,\n" +
                "       USER.NAME as USER_NAME,\n" +
                "       GOODS.NAME as GOODS_NAME,\n" +
                "       (\"ORDER\".GOODS_NUM*\"ORDER\".GOODS_PRICE) as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "join GOODS on \"ORDER\".GOODS_ID=GOODS.ID\n" +
                "join USER on \"ORDER\".USER_ID=USER.ID")) {
            List<Order> orders = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Order order = new Order(resultSet.getInt(1),
                        resultSet.getString(2),
                        resultSet.getString(3),
                        resultSet.getBigDecimal(4));
                orders.add(order);
            }
            return orders;
        }
    }

    /**
     *
     * @param databaseConnection 数据库连接
     * @return 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     * @throws SQLException 数据库连接异常
     */
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".ID as ORDER_ID,\n" +
                "       USER.NAME as USER_NAME,\n" +
                "       GOODS.NAME as GOODS_NAME,\n" +
                "       (\"ORDER\".GOODS_NUM*\"ORDER\".GOODS_PRICE) as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "left join GOODS on \"ORDER\".GOODS_ID=GOODS.ID\n" +
                "left join USER on \"ORDER\".USER_ID=USER.ID")) {
            List<Order> orders = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Order order = new Order(resultSet.getInt(1),
                        resultSet.getString(2),
                        resultSet.getString(3),
                        resultSet.getBigDecimal(4));
                orders.add(order);
            }
            return orders;
        }
    }
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
