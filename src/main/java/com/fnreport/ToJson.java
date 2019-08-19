package com.fnreport;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
    static final boolean USEJSONINPUT = Objects.equals(System.getenv("JSONINPUT"), "true");
    static final boolean ASYNC = Objects.equals(System.getenv("ASYNC"), "true");
    static long counter;

    static {
        System.setProperty("user.timezone", "UTC");
    }

    static public void main(String... args) throws Exception {
        if (args.length < 1) {
            System.err.println("copy all tables to json PUT\n\t  [ASYNC=true]  [JSONINPUT=true] " + ToJson.class.getCanonicalName() + " dbhost dbname user password couchprefix [jdbc:url:etc]");
            exit(1);
        }

        var jdbcurl = args.length > 5 ? args[5] : "jdbc:mysql://" + args[0] +
                "/" + args[1] +
                "?zeroDateTimeBehavior=convertToNull&user=" + args[2] +
                "&password=" + args[3];
        var DRIVER = DriverManager.getDriver(jdbcurl);

        System.err.println("\"use json rows\" is " + USEJSONINPUT);
        System.err.println("\"rest.async\" is " + ASYNC);
        var connect = DRIVER.connect(jdbcurl, new Properties());
        var metaData = connect.getMetaData();

        var sourceTables = metaData.getTables(null, null, null, new String[]{"TABLE"});

        var couchprefix = args[4];

        var tables = new ArrayList<String>();
        var c = 1;
        while (sourceTables.next()) {
            var table_schem = sourceTables.getString("TABLE_SCHEM");
            var table_name = sourceTables.getString("TABLE_NAME");
            if (table_schem != null && !table_schem.isEmpty()) {
                table_name = table_schem + '.' + table_name;
            }
            tables.add(table_name);
        }
        var gson = new ObjectMapper() {{
            setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        }};
        for (var tablename : tables) {
            var realm = tablename.split("\\.", 2);
            var name = realm.length > 1 ? realm[1] : tablename;
            var statement = connect.createStatement();

            try (var resultSet = statement.executeQuery("select count(*) from " + tablename)) {
                resultSet.next();
                final var aLong = resultSet.getLong(1);
                System.err.println("table: " + tablename + " " + aLong);
            }

            try (var resultSet = statement.executeQuery("select * from " + tablename)) {
                var metaData1 = resultSet.getMetaData();
                var columnCount = metaData1.getColumnCount();
                String pk = null;
                try (var primaryKeys = metaData.getPrimaryKeys(null, realm.length > 1 ? realm[0] : null, name)) {
                    primaryKeys.next();
                    pk = primaryKeys.getString(4);
                } catch (SQLException e) {
                    System.err.println("no pk for " + tablename);
                    /*
                    e.printStackTrace();
                    throw new Error("refine");
*/
                }
                var first = true;
                Map<Integer, AtomicInteger> responses = new LinkedHashMap<>();
                var tableAccessUrl = couchprefix + name.toLowerCase() + "/";

                Map<String, Integer> tablecreation = Collections.EMPTY_MAP;
                while (resultSet.next()) {
                    if (first) {
                        first = false;
                        var dest = couchprefix + name.toLowerCase().replaceAll("\\W+", "_");
                        var httpCon = (HttpURLConnection) new URL(dest).openConnection();

                        var utf8s = "{}".getBytes();
                        httpCon.setFixedLengthStreamingMode(utf8s.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);

                        try (var outputStream = httpCon.getOutputStream()) {
                            outputStream.write(utf8s);
                        }
                        //sync the table creation

                        System.err.println(Arrays.deepToString(
                                new Object[]{dest, httpCon.getResponseCode(),
                                        httpCon.getResponseMessage()}));


                        tablecreation = Collections.singletonMap("initial_access", httpCon.getResponseCode());

                        httpCon.disconnect();
                    }
                    Map<String, Object> row = new LinkedHashMap<>();

                    for (var i = 1; i < columnCount + 1; ++i) {
                        var columnType = metaData1.getColumnClassName(i);
                        var columnName = metaData1.getColumnName(i);
                        try {
                            var object = resultSet.getObject(i);

                            if (object instanceof String && USEJSONINPUT) {
                                var s = String.valueOf(object).trim();
                                if (!s.isBlank())
                                    switch (new StringBuilder().append(s.charAt(0)).append(s.charAt(s.length() - 1)).toString()) {
                                        case "[]":
                                            object = gson.readValue(s, Array.class);
                                            break;
                                        case "{}":
                                            object = gson.readValue(s, Map.class);
                                            break;
                                        default:
                                            break;
                                    }
                            }
                            row.put(columnName, object);
                        } catch (SQLException e) {
                            System.err.println("cannot store " + columnType + " as column " + columnName + " with value ");
                            row.put(columnName, resultSet.getString(i));
                        }
                    }


                    var str = pk == null ? Long.toHexString(++counter | 0x1000000000L).substring(1) : resultSet.getString(pk);
                    HttpURLConnection httpCon = null;
                    try {
                        httpCon = (HttpURLConnection) new URL(tableAccessUrl + str).openConnection();
                        var utf8s = gson.writeValueAsBytes(row);
                        httpCon.setFixedLengthStreamingMode(utf8s.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);
                        try (var outputStream = httpCon.getOutputStream()) {
                            outputStream.write(utf8s);
                            outputStream.flush();
                        }
                    } finally {
                        if (!ASYNC) {
                            var responseCode = httpCon.getResponseCode();
                            responses.computeIfAbsent(responseCode, initialValue -> new AtomicInteger(0)).incrementAndGet();
                        }
                        httpCon.disconnect();

                    }
                }


                System.err.println(Arrays.deepToString(new Object[]{tableAccessUrl, tablecreation, responses}));
            }
        }
    }
}
