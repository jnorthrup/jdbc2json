//package com.fnreport;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.arrow.adapter.jdbc.JdbcToArrow;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.VectorSchemaRoot;
//import org.apache.arrow.vector.dictionary.DictionaryProvider;
//import org.apache.arrow.vector.ipc.ArrowFileWriter;
//
//import java.io.IOException;
//import java.sql.*;
//import java.text.MessageFormat;
//import java.text.SimpleDateFormat;
//import java.util.Arrays;
//import java.util.List;
//import java.util.StringJoiner;
//
//public class q2f {
//
//    public static void main(String[] args) {
//        if (args.length < 1) {
//            System.err.println(MessageFormat.format("dump query to stdout or  $OUTPUT \n" +
//                    " [TABLE='tablename'] [OUTPUT='outfilename.txt'] {0} ''jdbc-url'' <sql>   ", q2f.class));
//            System.exit(1);
//        }
//        String jdbcUrl = args[0];
//
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
//        StringJoiner stringJoiner = new StringJoiner(" ");
//        Arrays.asList(args).subList(1, args.length).forEach(stringJoiner::add);
//        String sql = stringJoiner.toString( );
//        System.err.println("using sql: " + sql);
//        try {
//            Driver driver = DriverManager.getDriver(jdbcUrl);
//            Connection connect = driver.connect(jdbcUrl, System.getProperties());
//            Statement statement = connect.createStatement();
//            ResultSet rs = statement.executeQuery(sql);
//
//            VectorSchemaRoot sqlToArrow = JdbcToArrow.sqlToArrow(rs);
//            new ArrowFileWriter(sqlToArrow, new DictionaryProvider.MapDictionaryProvider(),os)
////            System.err.println(sqlToArrow.contentToTSVString());
//         } catch (SQLException | IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//}
