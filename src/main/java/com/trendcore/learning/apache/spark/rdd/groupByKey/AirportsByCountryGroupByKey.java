package com.trendcore.learning.apache.spark.rdd.groupByKey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class AirportsByCountryGroupByKey {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AirportsByCountry").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("in/airports.text");

        JavaRDD<String> rdd1 = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true);

        /**
         * Group by key is present on pair rdd only
         */

        String headerRow = rdd1.first();
        /**
         * groupBy -> will return pair rdd <v1[3] i.e country as key , v1 as value>
         */
        JavaPairRDD<String, Iterable<String[]>> groupByRdd = rdd1
                .filter(v1 -> !v1.equals(headerRow))
                .map(v1 -> v1.split(","))
                .groupBy(v1 -> v1[3]);
        System.out.println(rdd1);

        groupByRdd.foreach(stringIterableTuple2 -> {
            String key = stringIterableTuple2._1;
            Iterable<String[]> strings = stringIterableTuple2._2;
            System.out.println(key + " " + strings);
        });

        /**
         * 2nd approach by pair rdd.
         */
        rdd1
        .filter(v1 -> !v1.equals(headerRow))
        .mapToPair(s -> {
            String[] split = s.split(",");
            return new Tuple2<>(split[3],split);
        })
        .groupByKey()
        .foreach(stringIterableTuple2 -> {
            String s = stringIterableTuple2._1;
            Iterable<String[]> strings = stringIterableTuple2._2;
            System.out.println(s + " " + strings);
        });

    }

}
