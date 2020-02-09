package com.trendcore.learning.apache.spark.rdd.aggregation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.collection.Iterator$;

import java.util.Arrays;

public class WordCountReduceByKey {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountReduceByKey")
                .setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext
                .textFile("in/word_count.text");


        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rdd
                .map(v1 -> v1.split(" "))
                .flatMap(strings -> Arrays.stream(strings).iterator())
                .mapToPair(strings -> new Tuple2<>(strings, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        stringIntegerJavaPairRDD.foreach(stringIntegerTuple2 -> {
            System.out.println(stringIntegerTuple2._1 + " " + stringIntegerTuple2._2);
        });


    }

}
