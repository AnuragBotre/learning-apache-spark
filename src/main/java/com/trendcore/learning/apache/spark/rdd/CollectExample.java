package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class CollectExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                                .setAppName("CollectExample")
                                .setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        String vals[] = { "spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop" };

        sparkContext
                .parallelize(Arrays.asList(vals))
                .collect().forEach(s -> {
            System.out.println(s);
        });
    }
}
