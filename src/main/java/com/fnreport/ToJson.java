package com.fnreport;

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


    static long counter;
    public static final byte[] BYTES = new byte[4096];

    static public void main(String... args) throws IllegalAccessException, InstantiationException, SQLException, IOException {
        String jdbcurl = null;
        Driver DRIVER;
        if (args.length > 5) {
            DRIVER = DriverManager.getDriver(jdbcurl = args[6]);
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
        for (boolean valid = sourceTables.first(); valid; valid = sourceTables.next()) {
            tables.add(sourceTables.getString("TABLE_NAME"));
        }

//        Map<String, Map> rows = new LinkedHashMap<String, Map>();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        for (String name : tables) {
            Statement statement = connect.createStatement();

            ResultSet resultSet = statement.executeQuery("select * from " + name);
            ResultSetMetaData metaData1 = resultSet.getMetaData();
            int columnCount = metaData1.getColumnCount();

            String pk = null;
            try {
                ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, name);
                primaryKeys.next();
                pk = primaryKeys.getString(4);
            } catch (SQLException e) {

            }

            boolean valid = resultSet.first();
            while (valid) {
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
                String concat = name + ("_") + (str);
                URL url = new URL(new StringBuilder().append(args[4]).append('/').append(concat).toString());
                HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                byte[] utf8s = gson.toJson(row).getBytes("UTF8");
                httpCon.setFixedLengthStreamingMode(utf8s.length);
                httpCon.setRequestMethod("PUT");
                httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                httpCon.getOutputStream().write(utf8s);
                httpCon.getOutputStream().flush();
                httpCon.disconnect();

                valid = resultSet.next();
            }


        }


//        gson.toJson(rows, new FileWriter(args[1] + ".json"));

    }

}



