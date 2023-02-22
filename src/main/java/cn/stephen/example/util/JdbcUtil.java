package cn.stephen.example.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    static {
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<JSONObject> executeSQLServerQuery(String hostUrl, int port, String user, String password, String sql){
        List<JSONObject> beJson = new ArrayList<>();
        String connectionUrl = String.format("jdbc:sqlserver://%s:%s",hostUrl,port);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl,user,password);
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            beJson = resultSetToJson(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return beJson;
    }

    private static List<JSONObject> resultSetToJson(ResultSet rs) throws SQLException {
        List<JSONObject> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }

    public static void main(String[] args) {
        String host = "hadoop001";
        String user = "sa";
        String password = "@root123456";
        String sql = "SELECT TABLE_SCHEMA,TABLE_NAME FROM inventory.information_schema.tables WHERE TABLE_SCHEMA = 'dbo'";
        List<JSONObject> results = executeSQLServerQuery(host, 1433, user, password, sql);
        for(JSONObject jsob : results) {
            String schemaName = jsob.getString("TABLE_SCHEMA");
            String tblName = jsob.getString("TABLE_NAME");
            String schemaTbl = schemaName + "." + tblName;

            System.out.println("schemaName: " + schemaName + "\t" + "tblName: " + tblName + "\t" + "schemaTbl: " + schemaTbl);
        }

    }

}
