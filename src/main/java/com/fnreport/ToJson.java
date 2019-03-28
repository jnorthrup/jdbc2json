package com.fnreport;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.exit;

/**
 * Created by IntelliJ IDEA.
 * User: jim
 * Date: 11/30/11
 * Time: 12:53 AM
 */
public class ToJson {
    static final GsonBuilder BUILDER =
            new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setFieldNamingPolicy(
                    FieldNamingPolicy.IDENTITY).setPrettyPrinting();
    static long counter;

    static {
        System.setProperty("user.timezone", "UTC");
    }

    static public void main(String... args) throws Exception {
        if (args.length < 1) {
            System.err.println("copy all tables to json PUT\n\t" + ToJson.class.getCanonicalName() + " dbhost dbname user password couchprefix [jdbc:url:etc]");
            exit(1);
        }
        Driver DRIVER;
        String jdbcurl = args.length > 5 ? args[5] : "jdbc:mysql://" + args[0] +
                "/" + args[1] +
                "?zeroDateTimeBehavior=convertToNull&user=" + args[2] +
                "&password=" + args[3];
        DRIVER = DriverManager.getDriver(jdbcurl);

        Connection connect = DRIVER.connect(jdbcurl, new Properties());
        DatabaseMetaData metaData = connect.getMetaData();

        ResultSet sourceTables = metaData.getTables(null, null, null, new String[]{"TABLE"});

        String couchprefix = args[4];

        List<String> tables = new ArrayList<String>();
        int c = 1;
        while (sourceTables.next()) {
            String table_schem = sourceTables.getString("TABLE_SCHEM");
            String table_name = sourceTables.getString("TABLE_NAME");
            if (table_schem != null && !table_schem.isEmpty()) {
                table_name = table_schem + '.' + table_name;
            }
            tables.add(table_name);
        }

//        Map<String, Map> rows = new LinkedHashMap<String, Map>();
        Gson gson = BUILDER.create();//new GsonBuilder().setPrettyPrinting().create();

        for (String tablename : tables) {
            String[] split = tablename.split("\\.", 2);
            String name = split.length > 1 ? split[1] : tablename;
            Statement statement = connect.createStatement();


            try (ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename)) {
                resultSet.next();
                final long aLong = resultSet.getLong(1);
                System.err.println("table: " + tablename + " " + aLong);
                resultSet.close();
            }

            try (ResultSet resultSet = statement.executeQuery("select * from " + tablename)) {
                ResultSetMetaData metaData1 = resultSet.getMetaData();
                int columnCount = metaData1.getColumnCount();
                String pk = null;
                try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, split.length > 1 ? split[0] : null, name)) {
                    primaryKeys.next();
                    pk = primaryKeys.getString(4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new Error("refine");
                }
                boolean first = true;
                Map<Integer, AtomicInteger> responses = new LinkedHashMap<>();
                String tableAccessUrl = couchprefix + name + "/";

                Map<String, Integer> tablecreation = Collections.EMPTY_MAP;
                while (resultSet.next()) {
                    if (first) {
                        first = false;
                        HttpURLConnection httpCon = (HttpURLConnection) new URL(couchprefix + name).openConnection();
                        byte[] utf8s = "{}".getBytes();
                        httpCon.setFixedLengthStreamingMode(utf8s.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);
                        try (OutputStream outputStream = httpCon.getOutputStream()) {
                            outputStream.write(utf8s);
                            outputStream.flush();
                        }

                        tablecreation = Collections.singletonMap("initial_access", httpCon.getResponseCode());

                        httpCon.disconnect();
                    }
                    Map<String, Object> row = new LinkedHashMap<>();

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


                    String str = pk == null ? Long.toHexString(++counter | 0x1000000000L).substring(1) : resultSet.getString(pk);
                    HttpURLConnection httpCon = null;
                    try {
                        httpCon = (HttpURLConnection) new URL(tableAccessUrl + str).openConnection();
                        byte[] utf8s = gson.toJson(row).getBytes(StandardCharsets.UTF_8);
                        //                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                        httpCon.setFixedLengthStreamingMode(utf8s.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);
                        //                gson.toJson(Arrays.asList(url, row), System.out);
                        try (OutputStream outputStream = httpCon.getOutputStream()) {
                            outputStream.write(utf8s);
                            outputStream.flush();
                        }
                    } finally {
                        int responseCode = httpCon.getResponseCode();
                        responses.computeIfAbsent(responseCode, initialValue -> new AtomicInteger(0)).incrementAndGet();
                        httpCon.disconnect();

                    }
                }
                System.err.println(Arrays.deepToString(new Object[]{tableAccessUrl, tablecreation, responses}));
            }
        }
    }
}
