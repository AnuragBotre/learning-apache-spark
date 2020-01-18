package com.trendcore.learning.apache.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SkipNRowsFromRdd {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("SameHostsProblem").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> firstHostRdd = sparkContext.textFile("in/nasa_19950701.tsv");

        firstHostRdd.mapPartitionsWithIndex((v1, v2) -> {
            if(v1 == 0){
                String next = v2.next();
                System.out.println("This will skipped :: " + next);
            }
            return v2;
        },true).foreach(s -> System.out.println(s));
    }
}
