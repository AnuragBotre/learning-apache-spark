package com.trendcore.learning.apache.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 *        "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 *        Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
 *        Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
 *
 *        Example output:
 *        vagrant.vf.mmc.com
 *        www-a1.proxy.aol.com
 *        .....
 *
 *        Keep in mind, that the original log files contains the following header lines.
 *        host	logname	time	method	url	response	bytes
 *
 *        Make sure the head lines are removed in the resulting RDD.
 */
public class SameHostsProblem {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("SameHostsProblem").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> firstHostRdd = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> secondHostRdd = sparkContext.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> hostInFirstFile = firstHostRdd.map(v1 -> v1.split("\t")[0]);
        JavaRDD<String> hostInSecondFile = secondHostRdd.map(v1 -> v1.split("\t")[0]);

        JavaRDD<String> intersection = hostInFirstFile.intersection(hostInSecondFile);

        intersection.filter(v1 -> !"host".equals(v1)).saveAsTextFile("output/nasa_logs_same_hosts.csv");
    }
}
