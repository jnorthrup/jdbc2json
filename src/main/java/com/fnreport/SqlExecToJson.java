package com.fnreport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.exit;
import static java.util.Arrays.*;


/**
 * Created by IntelliJ IDEA.
 * User: jim
 * Date: 11/30/11
 * Time: 12:53 AM
 */
public class SqlExecToJson {
    public static final boolean USEJSONINPUT = Objects.equals(System.getenv("JSONINPUT"), "true");
    public static final boolean ASYNC = Objects.equals(System.getenv("ASYNC"), "true");
//    public static final GsonBuilder BUILDER = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setFieldNamingPolicy(
//            FieldNamingPolicy.IDENTITY).setPrettyPrinting();
    public static final ObjectMapper GSON = new ObjectMapper(){{
        setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
//        setPropertyNamingStrategy(PropertyNamingStrategy.)
}};
    static long counter;

    static {
        System.setProperty("user.timezone", "UTC");
    }

    static public void main(String... args) {
        if (args.length < 1) {
            System.err.println(MessageFormat.format("convert a query to json (and PUT to url) \n [ASYNC=true] [JSONINPUT=true] {0} name pkname couch_prefix ''jdbc-url''  <sql>   ", SqlExecToJson.class.getCanonicalName()));
            exit(1);
        }
        System.err.println("\"use json rows\" is " + USEJSONINPUT);
        System.err.println("\"rest.async\" is " + ASYNC);

        final String couchDbName = args[0];
        final String pkname = args[1];
        final String couchPrefix = args[2];
        final String jdbcUrl = args[3];
        StringJoiner stringJoiner = new StringJoiner(" ");
        asList(args).subList(4, args.length).forEach(stringJoiner::add);
        String sql = stringJoiner.toString();
        System.err.println("using sql: " + sql);
        Driver DRIVER = null;
        try {
            DRIVER = DriverManager.getDriver(jdbcUrl);
        } catch (SQLException e) {
            e.printStackTrace();
            exit(1);

        }
        ResultSetMetaData metaData1 = null;
        try (var resultSet = DRIVER.connect(jdbcUrl, new Properties()).createStatement().executeQuery(sql)) {
            metaData1 = resultSet.getMetaData();
            int columnCount = 0;int responseCode = 0;
            try {
                columnCount = metaData1.getColumnCount();

                boolean first = true;
                LinkedHashMap<Integer, AtomicInteger> responses = new LinkedHashMap<>();
                while (true) {
                    try {
                        if (!resultSet.next()) break;
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    if (first) {
//                             String str = (pk == null) ? Long.toHexString((++counter) | 0x1000000000l).substring(1) : resultSet.getString(pk);
                        first = false;
                        String spec = new StringBuilder().append(couchPrefix).append(couchDbName).toString();
                        Pattern compile = Pattern.compile("(http[s]?://)([^:]+:[^@]+)@(.*)");
                        Matcher matcher = compile.matcher(spec);
                        String basic = null;
                        if (matcher.matches()) {

                            basic = "Basic " + new String(Base64.getEncoder().encode(matcher.group(2).getBytes()));
spec=matcher.group(1)+matcher.group(3);

                        }

                        URL url = new URL(spec);
                        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
                        if (null != basic) {
                            httpCon.setRequestProperty("Authorization", basic);
                        }
                        byte[] utf8s = "{}".getBytes();
//                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                        httpCon.setFixedLengthStreamingMode(utf8s.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                        httpCon.getOutputStream().write(utf8s);
                        httpCon.getOutputStream().flush();
                        if (!ASYNC) responseCode = httpCon.getResponseCode();
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
                                    try {
                                        switch (new StringBuilder().append(s.charAt(0)).append(s.charAt(s.length() - 1)).toString()) {
                                            case "[]":
                                                object = GSON.readValue(s, List.class);
                                                break;
                                            case "{}":
                                                object = GSON.readValue(s, Map.class);
                                                break;
                                            default:
                                                break;
                                        }
                                    } catch (Throwable e) {
                                        System.err.println(s);
                                        System.err.println(e.getMessage());

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


                    var url = new URL(spec);
                    HttpURLConnection httpCon = null;
                    try {

                        httpCon = (HttpURLConnection) url.openConnection();

                        byte[] bytes = new byte[0];
                        try {
                            bytes = GSON.writeValueAsBytes(row);//.getBytes(StandardCharsets.UTF_8);
                        } catch (Exception e) {

                            System.err.printf("%s :%s%n", e.getMessage(), deepToString(asList(row).toArray()));
                        } finally {
                        }
//                httpCon.getRequestProperties().put("Content-Type", asList("application/json"))  ;
                        httpCon.setFixedLengthStreamingMode(bytes.length);
                        httpCon.setRequestMethod("PUT");
                        httpCon.setUseCaches(true);
                        httpCon.setDoOutput(true);
//                gson.toJson(Arrays.asList(url, row), System.out);
                        httpCon.getOutputStream().write(bytes);
                        httpCon.getOutputStream().flush();

                        if (!ASYNC) {
                            responseCode = httpCon.getResponseCode();
                        responses.computeIfAbsent(responseCode, initialValue -> new AtomicInteger(0)).incrementAndGet();

                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        httpCon.disconnect();
                    }
                }

                System.err.println(deepToString(asList(responses).toArray()));
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ProtocolException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}
