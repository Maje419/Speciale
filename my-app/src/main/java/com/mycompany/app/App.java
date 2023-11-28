package com.mycompany.app;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Tests how the flow works if one connection commits an update to the database
 * while another connection has already been opened
 * Result: open connections are updated live
 *
 */
public class App {
    public static void main(String[] args) throws SQLException {
        String connectionString = "jdbc:postgresql:///example-db";
        Connection con1 = DriverManager.getConnection(
                connectionString,
                "postgres",
                "example");

        Connection con2 = DriverManager.getConnection(
                connectionString,
                "postgres",
                "example");

        con2.setAutoCommit(false);

        PreparedStatement update = con1.prepareStatement("insert into test(lol) values(3)");
        update.executeUpdate();

        PreparedStatement query = con2.prepareStatement("SELECT * FROM test;");
        ResultSet rs = query.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("lol"));
        }
        // con.commit();

    }
}
