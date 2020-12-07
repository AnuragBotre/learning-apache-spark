package com.trendcore.learning.apache.spark.connector;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.JdbcRDD;
import scala.Function0;
import scala.Function1;
import scala.Serializable;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcRddExample {

    public static void main(String[] args) throws SQLException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("JDBC Rdd Example");
                //.setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(sparkConf);


        /**
         * param: lowerBound the minimum value of the first placeholder
         * param: upperBound the maximum value of the second placeholder
         * The lower and upper bounds are inclusive.
         * param: numPartitions the number of partitions.
         * Given a lowerBound of 1, an upperBound of 20,
         * and a numPartitions of 2,
         * the query would be executed twice,
         * once with (1, 10) and once with (11, 20)
         *
         * param: mapRow a function from a ResultSet to a
         * single row of the desired result type(s).
         * This should only call getInt, getString, etc; the RDD takes care of calling next.
         * The default maps a ResultSet to an array of Object.
         */

        ConnectionCreator connectionCreator = new ConnectionCreator();
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD(sparkContext,
                connectionCreator,
                "select * from actor a where a.actor_id >= ? and  a.actor_id <= ?",
                1, 20, 2,
                    resultSetMapper
                    , ClassTag.apply(Object[].class));

        // Convert to JavaRDD
        //JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassTag.apply(Object[].class));

        List<Object[]> collect = javaRDD.collect();

        collect.forEach(objects -> {
            for (Object object : objects) {
                System.out.print(object);
                System.out.print(" ");
            }
            System.out.println();
        });

    }

    public static class ConnectionCreator implements Function0<Connection> ,Serializable {

        @Override
        public Connection apply() {
            try {
                return DriverManager.getConnection("jdbc:mysql://localhost:3306/sakila", "anurag", "anurag");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ResultSetMapper implements Function1<ResultSet,Object[]>, Serializable{

        @Override
        public Object[] apply(ResultSet v1) {
            return JdbcRDD.resultSetToObjectArray(v1);
        }


    }

}
