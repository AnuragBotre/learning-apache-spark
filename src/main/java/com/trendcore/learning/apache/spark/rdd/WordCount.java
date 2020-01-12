package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        SparkConf wordCountSparkConf = new SparkConf().setAppName("WordCount").setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(wordCountSparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/word_count.text");

        List<Tuple2<String, Integer>> collect = stringJavaRDD.flatMap(s ->
                Arrays.asList(s.split(" ")).iterator()
        ).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((v1, v2) -> v1 + v2).collect();

        collect.forEach(stringIntegerTuple2 ->
                System.out.println(stringIntegerTuple2._1 +" " + stringIntegerTuple2._2));

    }

}
