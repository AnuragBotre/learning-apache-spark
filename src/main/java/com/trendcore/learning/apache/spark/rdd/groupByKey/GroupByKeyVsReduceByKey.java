package com.trendcore.learning.apache.spark.rdd.groupByKey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class GroupByKeyVsReduceByKey {

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

        rdd1.mapToPair(s -> {
            String[] split = s.split(",");
            ArrayList<Object> objects = new ArrayList<>();
            objects.add(split[1]);
            return new Tuple2<>(split[3], objects);
        }).reduceByKey((v1, v2) -> {
            v1.addAll(v2);
            return v1;
        }).foreach(stringListTuple2 -> {
            System.out.println(stringListTuple2._1 + ":" + stringListTuple2._2);
        });

        rdd1.map(v1 -> {
            String[] split = v1.split(",");
            return new Tuple2<>(split[3], split[1]);
        })
                .groupBy(v1 -> v1._1)
                .collectAsMap()
                .forEach((o, tuple2s) -> {
                    System.out.println(o + "::" + tuple2s);
                });


        JavaPairRDD<String, ArrayList<String>> stringArrayListJavaPairRDD = rdd1.mapToPair(v1 -> {
            String[] split = v1.split(",");
            return new Tuple2<>(split[3], split[1]);
        }).combineByKey(v1 -> {
            ArrayList<String> objects = new ArrayList<>();
            objects.add(v1);
            return objects;
        }, (v1, v2) -> {
            v1.add(v2);
            return v1;
        }, (v1, v2) -> {
            v1.addAll(v2);
            return v1;
        });

        stringArrayListJavaPairRDD.foreach(stringArrayListTuple2 -> {
            System.out.println(stringArrayListTuple2._1 + " " + stringArrayListTuple2._2);
        });

    }

}
