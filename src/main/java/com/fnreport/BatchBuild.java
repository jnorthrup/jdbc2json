package com.fnreport;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.exit;
import static java.util.Arrays.*;

/**
 * Created by IntelliJ IDEA.
 * User: jim
 * Date: 11/30/11
 * Time: 12:53 AM
 */
public class BatchBuild {
    public static final boolean USEJSONINPUT = Objects.equals(System.getenv("JSONINPUT"), "true");
    public static final boolean ASYNC = Objects.equals(System.getenv("ASYNC"), "true");
    public static final GsonBuilder BUILDER =
            new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setFieldNamingPolicy(
                    FieldNamingPolicy.IDENTITY).setPrettyPrinting();
    public static final Gson GSON = BUILDER.create();
    static long counter;

    static {
        System.setProperty("user.timezone", "UTC");
    }

    static public void main(String... args) throws Exception {
        if (args.length < 1) {
            System.err.println(MessageFormat.format("convert a query to json (and PUT to url) \n [ASYNC=true] [JSONINPUT=true] {0} name pkname couch_prefix ''jdbc-url''  <sql>   ", BatchBuild.class.getCanonicalName()));
            exit(1);
        }
        System.err.println("\"use json rows\" is " + USEJSONINPUT);
        System.err.println("\"rest.async\" is " + ASYNC);

        final String couchDbName = args[0];
        final String pkname = args[1];
        final String couchPrefix = args[2];
        final String jdbcUrl = args[3];
        StringJoiner stringJoiner = new StringJoiner(" ");
        asList(args).subList(4, args.length  ).forEach(stringJoiner::add);
        String sql = stringJoiner.toString();
        System.err.println("using sql: "+sql);
        Driver DRIVER = DriverManager.getDriver(jdbcUrl);
        final ResultSet resultSet = DRIVER.connect(jdbcUrl, new Properties()).createStatement().executeQuery(sql);
        ResultSetMetaData metaData1 = resultSet.getMetaData();
        int columnCount = metaData1.getColumnCount();

        boolean first = true;
        LinkedHashMap<Integer, AtomicInteger> responses = new LinkedHashMap<>();
        while (resultSet.next()) {
            if (first) {
//                             String str = (pk == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pk);
                first = false;
                String spec = new StringBuilder().append(couchPrefix).append(couchDbName).toString();
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

                    if (object instanceof String && USEJSONINPUT) {
                        String s = String.valueOf(object).trim();
                        if (!s.isBlank())
                            switch (new StringBuilder().append(s.charAt(0)).append(s.charAt(s.length() - 1)).toString()) {
                                case "[]":
                                    object = GSON.fromJson(s, List.class);
                                    break;
                                case "{}":
                                    object = GSON.fromJson(s, Map.class);
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

            String _id = pkname.isBlank() ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pkname);
            String spec = new StringBuilder().append(couchPrefix).append(couchDbName).append("/").append(_id).toString();
            URL url = new URL(spec);
            HttpURLConnection httpCon = null;
            try {

                httpCon = (HttpURLConnection) url.openConnection();

                byte[] utf8s = GSON.toJson(row).getBytes(StandardCharsets.UTF_8);
//                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                httpCon.setFixedLengthStreamingMode(utf8s.length);
                httpCon.setRequestMethod("PUT");
                httpCon.setUseCaches(true);
                httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                httpCon.getOutputStream().write(utf8s);
                httpCon.getOutputStream().flush();
                if (!ASYNC) {
                    int responseCode = httpCon.getResponseCode();
                    responses.computeIfAbsent(responseCode, initialValue -> new AtomicInteger(0)).incrementAndGet();
                }
            } finally {
                httpCon.disconnect();
            }
        }
        System.err.println(deepToString(asList(responses).toArray()));

    }
}
