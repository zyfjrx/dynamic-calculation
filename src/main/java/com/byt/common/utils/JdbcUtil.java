package com.byt.common.utils;

import com.byt.tagcalculate.pojo.TagProperties;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    private static BasicDataSource dataSource = new BasicDataSource();

    static {
        initSourceConfig();
    }

    private static void initSourceConfig() {
//        dataSource.setDriverClassName("org.postgresql.Driver");
//        dataSource.setUsername(ConfigManager.getProperty("greenplum.opc.user"));
//        dataSource.setPassword(ConfigManager.getProperty("greenplum.opc.pass"));
//        dataSource.setUrl(ConfigManager.getProperty("greenplum.opc.url"));

        // TODO 解决中文名称查询不到结果问题
        String url = "jdbc:mysql://"+ConfigManager.getProperty("mysql.host")+":"+ConfigManager.getProperty("mysql.port")+"/"+ConfigManager.getProperty("mysql.database")+"?autoReconnect=true&useSSL=false&characterEncoding=utf-8";
        System.out.println(url);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername(ConfigManager.getProperty("mysql.username"));
        dataSource.setPassword(ConfigManager.getProperty("mysql.password"));
        dataSource.setUrl(url);

        dataSource.setInitialSize(2);
        dataSource.setMaxTotal(4);
        dataSource.setMaxIdle(2);
        dataSource.setMinIdle(2);
        dataSource.setMaxWaitMillis(6000);
    }

    public static BasicDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接串
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                //将Connection连接对象还给数据库连接池
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean isUnderScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        ArrayList<T> list = new ArrayList<>();

        //查询
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历封装T对象放入集合  行遍历
        while (resultSet.next()) {

            //创建T对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {   //列遍历
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                if (isUnderScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //给T对象属性赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //将T对象放入集合
            list.add(t);
        }

        //释放资源
        resultSet.close();
        preparedStatement.close();

        return list;
    }

    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection("jdbc:mysql://ai3:3306?useSSL=false", "root", "rootroot");

        List<TagProperties> queryList = queryList(connection, "select * from `byt_grid_data`.tag_conf", TagProperties.class, false);
        for (TagProperties jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();

    }



}
