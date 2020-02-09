package com.trendcore.learning.apache.spark.rdd.groupByKey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AirportByCountryApproach2 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("AirportByCountryApproach2")
                .setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sparkContext.textFile("in/airports.text");

        Map<String, Iterable<String[]>> stringIterableMap = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true).map(v1 -> {
            String[] s = v1.split(",");
            return new String[]{s[1],s[3]};
        }).groupBy(v1 -> v1[1]).collectAsMap();

        stringIterableMap.forEach((s, lists) -> {
            System.out.println(s + " " + lists);
        });
    }

}
