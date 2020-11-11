package com.trendcore.learning.apache.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 * Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
 * take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
 * <p>
 * Keep in mind, that the original log files contains the following header lines.
 * host	logname	time	method	url	response	bytes
 * <p>
 * Make sure the head lines are removed in the resulting RDD.
 */
public class UnionLogProblem {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("UnionLogProblem").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd1 = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> rdd2 = sparkContext.textFile("in/nasa_19950801.tsv");

        /**
         * This solution is based on stackoverflow.
         * https://stackoverflow.com/questions/27854919/how-do-i-skip-a-header-from-csv-files-in-spark
         * But use filter recommended
         */
        JavaRDD<String> headerRemovedFromRdd1 = rdd1.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true);

        JavaRDD<String> headerRemovedFromRdd2 = rdd2.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true);

        headerRemovedFromRdd1
                .union(headerRemovedFromRdd2)

                //https://stackoverflow.com/questions/41787775/rdd-sample-in-spark
                //for sample with replacement or without replacement
                .sample(true, 0.1)
                .saveAsTextFile("output/sample_nasa_logs.tsv");

    }
}
