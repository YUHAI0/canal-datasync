package com;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;

@Slf4j
@Component
public class SqlHelper {

    public static void main(String[] args) {
    }

    @Autowired
    private CanalSinkConfig canalSinkConfig;

    public void doSql(String db, String sql) {
//        String db = canalSinkConfig.getDb();
        try {
            // 1.反射获取mysql驱动实例
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("找不到驱动程序类，加载驱动失败！");
            e.printStackTrace();
        }

        String url = String.format("jdbc:mysql://%s:%d/", canalSinkConfig.getHost(), canalSinkConfig.getPort());
        String username = canalSinkConfig.getUser();
        String password = canalSinkConfig.getPassword();

        try {
            // 2.驱动实例->Connection
            Connection conn = DriverManager.getConnection(url, username, password);
            conn.setCatalog(db);

            // 3.Connection->Statement
            Statement stmt = conn.createStatement();

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();

            log.info("Do sql: {}", sql);
            System.out.println(ps.getResultSet());

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
