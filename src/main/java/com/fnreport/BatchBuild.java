package com.fnreport;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.Date;

import static java.lang.System.exit;

/**
 * Created by IntelliJ IDEA.
 * User: jim
 * Date: 11/30/11
 * Time: 12:53 AM
 */
public class BatchBuild {
    static {
        System.setProperty("user.timezone", "UTC");
    }

    public static final GsonBuilder BUILDER =
            new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setFieldNamingPolicy(
                    FieldNamingPolicy.IDENTITY).setPrettyPrinting();
    static long counter;

    static public void main(String... args) throws IllegalAccessException, InstantiationException, SQLException, IOException {
        if (args.length < 1) {
            System.err.println(MessageFormat.format("convert a query to json (and PUT to url) \n{0} name ''sql'' pkname couch_prefix ''jdbc-url''", BatchBuild.class.getCanonicalName()));
        exit(1);}

        final String name = args[0];
        final String sql = args[1];
        final String pkname = args[2];
        final String couchPrefix = args[3];
        final String url = args[4];
        final ResultSet resultSet = DriverManager.getDriver(url).connect(url, new Properties()).createStatement().executeQuery(sql);
        doSelect(couchPrefix, name, resultSet, pkname);
    }

    private static void doSelect(String couchPrefix, String name, ResultSet resultSet1, String pkname) throws SQLException, IOException {
        ResultSetMetaData metaData1 = resultSet1.getMetaData();
        int columnCount = metaData1.getColumnCount();

        boolean first = true;
        while (resultSet1.next()) {
            if (first) {
//                             String str = (pk == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pk);
                first = false;
                String spec = new StringBuilder().append(couchPrefix).append(name).toString();
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
                    Object object = resultSet1.getObject(i);
                    if (object instanceof Date) {
                        Date date = (Date) object;


                        object = date.getTime();
                    }
                    row.put(columnName, object);

                } catch (SQLException e) {
                    System.err.println("cannot store " + columnType + " as column " + columnName + " with value ");
                    row.put(columnName, resultSet1.getString(i));
                }
            }


            final String string = resultSet1.getString(pkname);
            String _id = (pkname == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : string;
            String spec = new StringBuilder().append(couchPrefix).append(name).append("/").append(_id).toString();
            URL url = new URL(spec);
            HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
            byte[] utf8s = BUILDER.create().toJson(row).getBytes("UTF8");
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
