
package com.github.hcsp.sql;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SqlTest {
private static Connection connection;

@BeforeAll
public static void setConnection() throws SQLException {
    File projectDir = new File(System.getProperty("basedir", System.getProperty("user.dir")));
    String jdbcUrl = "jdbc:h2:file:" + new File(projectDir, "target/test").getAbsolutePath();
    connection = DriverManager.getConnection(jdbcUrl, "root", "Jxi1Oxc92qSj");
}

@AfterAll
public static void closeConnection() throws SQLException {
    if (connection != null) {
        connection.close();
    }
}

@Test
public void countUsersWhoHaveBoughtGoodsTest() throws Exception {
    System.out.println("countUsersWhoHaveBoughtGoodsTest");
    Assertions.assertEquals(2, Sql.countUsersWhoHaveBoughtGoods(connection, 1));
    Assertions.assertEquals(1, Sql.countUsersWhoHaveBoughtGoods(connection, 2));
}

@Test
public void getUsersByPageOrderedByIdDescTest() throws Exception {
    System.out.println("getUsersByPageOrderedByIdDescTest");
    List<Sql.User> users = Sql.getUsersByPageOrderedByIdDesc(connection, 2, 3);
    System.out.println(users);
    Assertions.assertEquals("1", users.stream().map(r -> r.id.toString()).collect(Collectors.joining(",")));

    users = Sql.getUsersByPageOrderedByIdDesc(connection, 2, 2);
    System.out.println(users);
    Assertions.assertEquals("2,1", users.stream().map(r -> r.id.toString()).collect(Collectors.joining(",")));
}

@Test
public void getGoodsAndGmvTest() throws Exception {
    System.out.println("getGoodsAndGmvTest");
    List<Sql.GoodsAndGmv> results = Sql.getGoodsAndGmv(connection);
    System.out.println(results);
    Assertions.assertEquals("2080,80,20,20", results.stream().map(r -> r.gmv.toString()).collect(Collectors.joining(",")));
}

@Test
public void getInnerJoinOrdersTest() throws Exception {
    System.out.println("getInnerJoinOrdersTest");
    List<Sql.Order> results = Sql.getInnerJoinOrders(connection);
    System.out.println(results);
    Assertions.assertEquals(6, results.size());
}

@Test
public void getLeftJoinOrdersTest() throws Exception {
    System.out.println("getLeftJoinOrdersTest");
    List<Sql.Order> results = Sql.getLeftJoinOrders(connection);
    System.out.println(results);
    Assertions.assertEquals(8, results.size());
}

}
