package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class CountExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                            .setAppName("CountExample")
                            .setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        String vals[] = { "spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop" };

        sparkContext.parallelize(Arrays.asList(vals))
                .mapToPair(t -> new Tuple2<>(t,1))
                .countByKey()
                .forEach((s, aLong) -> System.out.println(s+" : "+aLong));


        sparkContext.parallelize(Arrays.asList(vals))
                .countByValue()
                .forEach((s, aLong) -> System.out.println(s + " :: " + aLong));
    }

}
