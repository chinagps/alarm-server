package com.gps.alarm.placeserver;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

public class DataBase {

	private static Logger logger = Logger.getLogger(DataBase.class);

	private static BasicDataSource ds;

	public static void setDataSource(String driver, String url, String username, String password) {

		logger.info("connect to " + url + " with " + username + "/" + password + " dirver : " + driver);
		ds = new org.apache.commons.dbcp.BasicDataSource();

		((BasicDataSource) ds).setDriverClassName(driver);
		((BasicDataSource) ds).setUrl(url);
		((BasicDataSource) ds).setMaxActive(10);
		((BasicDataSource) ds).setMaxWait(100L);
		((BasicDataSource) ds).setUsername(username);
		((BasicDataSource) ds).setPassword(password);
	}

	public static void setDataSource(String url, String username, String password) {

		String driver = "oracle.jdbc.OracleDriver";
		logger.info("connect to " + url + " with " + username + "/" + password + " dirver(default) : " + driver);
		ds = new org.apache.commons.dbcp.BasicDataSource();

		((BasicDataSource) ds).setDriverClassName(driver);
		((BasicDataSource) ds).setUrl(url);
		((BasicDataSource) ds).setMaxActive(100);
		((BasicDataSource) ds).setMaxWait(100L);
		((BasicDataSource) ds).setUsername(username);
		((BasicDataSource) ds).setPassword(password);
	}

	public static void setDataSource(BasicDataSource basicDataSource) {

		ds = basicDataSource;

	}

	public synchronized static Connection getConnection() {
		Connection conn = null;

		try {
			conn = ds.getConnection();

		} catch (Exception e) {
			logger.error("", e);
		}

		return conn;
	}

	public static void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
			}
			rs = null;
		}
	}

	public static void close(PreparedStatement ps) {
		if (ps != null) {
			try {
				ps.close();
			} catch (Exception e) {
			}
			ps = null;
		}
	}

	public static void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
			}
			conn = null;
		}
	}

	public static void rollback(Connection conn) {
		if (conn != null) {
			try {
				conn.rollback();
			} catch (Exception e) {
			}
		}
	}

	public static void close(Statement st) {
		if (st != null) {
			try {
				st.close();
			} catch (Exception e) {
			}
		}
	}

}
