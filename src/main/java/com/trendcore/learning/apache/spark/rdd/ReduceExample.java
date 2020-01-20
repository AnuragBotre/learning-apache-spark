package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ReduceExample").setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Integer reduce = sparkContext
                .parallelize(Arrays.asList(1, 2, 3, 4, 5))
                .reduce((v1, v2) -> {
                    System.out.println(v1 + " " + v2);
                    return v1 + v2;
                });

        System.out.println(reduce);


        sparkContext
                .parallelize(Arrays.asList(1, 2, 3, 4, 5, 1, 2, 3, 5, 4))
                .mapToPair(integer -> new Tuple2<>(integer, 1))
                .reduceByKey((v1, v2) -> {
                    System.out.println("Key :: " + v1 + " value :: " + v2);
                    return v1 + v2;
                }).foreach(integerIntegerTuple2 -> {
            System.out.print("For Each");
            System.out.println(" Key :: " + integerIntegerTuple2._1 + " value :: " + integerIntegerTuple2._2);
        });
    }

}
