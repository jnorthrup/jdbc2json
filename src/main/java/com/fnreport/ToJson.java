package com.fnreport;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: jim
 * Date: 11/30/11
 * Time: 12:53 AM
 */
public class ToJson {

    public static final GsonBuilder BUILDER =
            new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setFieldNamingPolicy(
                    FieldNamingPolicy.IDENTITY).setPrettyPrinting();
    static long counter;

    static public void main(String... args) throws IllegalAccessException, InstantiationException, SQLException, IOException {
        String jdbcurl = null;
        Driver DRIVER;
        if (args.length > 5) {
            final String arg = args[5];
             DRIVER = DriverManager.getDriver(jdbcurl = arg);
        }
        else {
            DRIVER = com.mysql.jdbc.Driver.class.newInstance();
            jdbcurl = MessageFormat.format("jdbc:mysql://{0}/{1}?zeroDateTimeBehavior=convertToNull&user={2}&password={3}", args[0], args[1], args[2], args[3]);
        }

         Connection connect = DRIVER.connect(jdbcurl, new Properties());
        DatabaseMetaData metaData = connect.getMetaData();

        ResultSet sourceTables = metaData.getTables(null, null, null, new String[]{"TABLE"});


        List<String> tables = new ArrayList<String>();
        int c = 1;
        while(sourceTables.next())
        {
            String table_schem = sourceTables.getString("TABLE_SCHEM");
            String table_name = sourceTables.getString("TABLE_NAME");
            if(table_schem!=null&&!table_schem.isEmpty()) {table_name=table_schem+'.'+table_name;}
            {
                tables.add(table_name);
            }
        }

//        Map<String, Map> rows = new LinkedHashMap<String, Map>();
        Gson gson =BUILDER.create();//new GsonBuilder().setPrettyPrinting().create();

        for (String tablename : tables) {

            String[] split = tablename.split("\\.", 2);
            String name = split.length > 1 ? split[1] : tablename;
            Statement statement = connect.createStatement();

            ResultSet resultSet = statement.executeQuery("select * from " + tablename );
            ResultSetMetaData metaData1 = resultSet.getMetaData();
            int columnCount = metaData1.getColumnCount();

            String pk = null;
            try {
                ResultSet primaryKeys = metaData.getPrimaryKeys(null, split.length>1?split[0]:null, name);
                primaryKeys.next();
                pk = primaryKeys.getString(4);
            } catch (SQLException e) {

            }
            boolean first = true;
            while (  resultSet.next()) {
                         if(first){
//                             String str = (pk == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pk);
                             first=false;
                             String spec = new StringBuilder().append(args[4]).append('/').append(name).toString();
                             URL url = new URL(spec);
                             HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                             byte[] utf8s = "{}".getBytes();
//                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                             httpCon.setFixedLengthStreamingMode(utf8s.length);
                             httpCon.setRequestMethod("PUT");
                             httpCon.setUseCaches(true);
                             httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                             httpCon.getOutputStream().write(utf8s);
                             httpCon.getOutputStream().flush();
                             httpCon.disconnect();
                         }
                Map row = new LinkedHashMap();

                for (int i = 1; i < columnCount + 1; ++i) {
                    String columnType = metaData1.getColumnClassName(i);
                    String columnName = metaData1.getColumnName(i);
                    try {
                        Object object = resultSet.getObject(i);
                        row.put(columnName, object);

                    } catch (SQLException e) {
                        System.err.println("cannot store " + columnType + " as column " + columnName + " with value ");
                        row.put(columnName, resultSet.getString(i));
                    }
                }


                String str = (pk == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pk);
                String spec = new StringBuilder().append(args[4]).append('/').append(name).append("/") .append(str).toString();
                URL url = new URL(spec);
                HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                byte[] utf8s = gson.toJson(row).getBytes("UTF8");
//                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                httpCon.setFixedLengthStreamingMode(utf8s.length);
                httpCon.setRequestMethod("PUT");
                httpCon.setUseCaches(true);
                httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                httpCon.getOutputStream().write(utf8s);
                httpCon.getOutputStream().flush();
                httpCon.disconnect();
            }
        }
    }

}
