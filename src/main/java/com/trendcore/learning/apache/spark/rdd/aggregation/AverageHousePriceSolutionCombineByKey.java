package com.trendcore.learning.apache.spark.rdd.aggregation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class AverageHousePriceSolutionCombineByKey {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("AverageHousePriceSolutionCombineByKey")
                .setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("in/RealEstate.csv");

        JavaRDD<String> rdd1 = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true);

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rdd1.mapToPair(s -> {
            String[] split = s.split(",");
            return new Tuple2<>(split[3], 1);
        });


        /*stringIntegerJavaPairRDD.countByKey().forEach((s, aLong) -> {
            System.out.println(s + " :: " + aLong);
        });*/

        //Combine by key is same as above code + countByKey
        Map<String, Integer> stringLongMap = stringIntegerJavaPairRDD.combineByKey(v1 -> v1,
                (v1, v2) -> v1 + v2,
                (v1, v2) -> v1 + v2).collectAsMap();

        //Map<String, Long> stringLongMap = new HashMap<>();
        stringLongMap.forEach((s, aLong) -> {
            System.out.println(s + " " + aLong);
        });

        rdd1.mapToPair(s -> {
            String[] split = s.split(",");
            Double s1 = Double.parseDouble(split[2]);
            return new Tuple2<>(split[3], s1);
        }).combineByKey(v1 -> {
            System.out.println("createCombiner :: " + v1);
            return v1;
        }, (v1, v2) -> {
            System.out.println("mergeValue :: " + v1 + " " + v2);
            return v1 + v2;
        }, (v1, v2) -> {
            System.out.println("mergeCombiners :: " + v1 + " " + v2);
            return v1 + v2;
        }).foreach(stringDoubleTuple2 -> {
            Integer orDefault = stringLongMap.getOrDefault(stringDoubleTuple2._1, 1);
            double v = stringDoubleTuple2._2 / orDefault;
            System.out.println(stringDoubleTuple2._1 + " :: " + v );
        });


    }
}
