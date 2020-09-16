package com.trendcore.learning.apache.spark;

import org.apache.spark.SparkConf;

public class ClassLoadingMainMethod {

    public static void mainMethod(String[] args) {
        System.out.println(ClassLoadingMainMethod.class.getClassLoader());
        SparkConf wordCountSparkConf = new SparkConf().setAppName("WordCount").setMaster("local[3]");
        System.out.println(wordCountSparkConf.getClass().getClassLoader());
    }
}
