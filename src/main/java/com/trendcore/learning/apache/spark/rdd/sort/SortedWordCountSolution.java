package com.trendcore.learning.apache.spark.rdd.sort;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SortedWordCountSolution {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf()
                .setAppName("SortedWordCountSolution")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sparkContext.textFile("in/word_count.text");

        rdd.map(v1 -> v1.split(" "))
                .flatMap(strings -> Arrays.asList(strings).iterator())
                .mapToPair(s -> new Tuple2<>(s.trim(), 1))
                /*.combineByKey(v1 -> v1,
                        (v1, v2) -> v1 + v2,
                        (v1, v2) -> v1 + v2)*/
                .reduceByKey((v1, v2) -> v1 + v2)
                .map(v1 -> new Tuple2<>(v1._1, v1._2))
                .sortBy(v1 -> v1._2, false, rdd.partitions().size())
                .collect()
                .forEach(stringIntegerTuple2 -> {
                    System.out.println(stringIntegerTuple2._1 + " " + stringIntegerTuple2._2);
                });
    }

}
