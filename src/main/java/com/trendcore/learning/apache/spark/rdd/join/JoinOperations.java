package com.trendcore.learning.apache.spark.rdd.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class JoinOperations {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("JoinOperations").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Tuple2<String, Integer> tom = new Tuple2<>("Tom", 29);
        Tuple2<String, Integer> john = new Tuple2<>("John", 22);
        JavaPairRDD<String, Integer> ages = sparkContext.parallelizePairs(Arrays.asList(tom, john));

        Tuple2<String, String> tom1 = new Tuple2<>("James", "USA");
        Tuple2<String, String> john1 = new Tuple2<>("John", "UK");
        JavaPairRDD<String, String> addresses = sparkContext.parallelizePairs(Arrays.asList(tom1, john1));


        /**
         * This is inner join. not a cross join.
         */
        System.out.println("---------------------- Simple Join or Inner Join ----------------------");
        ages.join(addresses)
                .foreach(stringTuple2Tuple2 -> {
                    /**
                     *  Note in case of join 2 arg in tuple is important as it will contain rest of the columns.
                     */
                    System.out.println(stringTuple2Tuple2._1 + " " + stringTuple2Tuple2._2);
                });

        System.out.println("---------------------- Simple Join or Inner Join extracting column ----------------------");
        ages.join(addresses)
                .map(v1 -> {
                    /**
                     *  v1._2 is tuple with remain column.
                     */
                    return new Tuple2(v1._1, v1._2._2);
                })
                .foreach(stringTuple2Tuple2 -> {
                    /**
                     *  Note in case of join 2 arg in tuple is important as it will contain rest of the columns.
                     */
                    System.out.println(stringTuple2Tuple2._1 + " " + stringTuple2Tuple2._2);
                });

        /**
         * This will print 3 rows as there total 4 rows with 1 John being common.
         */
        System.out.println("---------------------- Full Outer Join ----------------------");
        ages.fullOuterJoin(addresses)
                .foreach(stringTuple2Tuple2 -> {
                    /**
                     *  Note in case of join 2 arg in tuple is important as it will contain rest of the columns.
                     */
                    System.out.println(stringTuple2Tuple2._1 + " " + stringTuple2Tuple2._2);
                });

        /**
         * This will print 2 rows from ages as its left outer join.
         */
        System.out.println("---------------------- Left Outer Join ----------------------");
        ages.leftOuterJoin(addresses).foreach(stringTuple2Tuple2 -> {
            /**
             *  Note in case of join 2 arg in tuple is important as it will contain rest of the columns.
             */
            System.out.println(stringTuple2Tuple2._1 + " " + stringTuple2Tuple2._2);
        });


        /**
         * This will print 2 rows from addresses as its right outer join.
         */
        System.out.println("---------------------- Right Outer Join ---------------------- ");
        ages.rightOuterJoin(addresses).foreach(stringTuple2Tuple2 -> {

            /**
             *  Note in case of join 2 arg in tuple is important as it will contain rest of the columns.
             */
            System.out.println(stringTuple2Tuple2._1 + " " + stringTuple2Tuple2._2);
        });
    }

}
