package com.trendcore.learning.apache.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RemoveHeaderFromCSV {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("UnionLogProblem").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd1 = sparkContext.textFile("in/nasa_19950701.tsv");

        /**
         * This solution is based on stackoverflow.
         * https://stackoverflow.com/questions/27854919/how-do-i-skip-a-header-from-csv-files-in-spark
         * But use filter recommended
         */

        /**
         * https://stackoverflow.com/questions/33655920/when-to-use-mapparitions-and-mappartitionswithindex
         * mapPartitions and mapPartitionsWithIndex pretty much do the same thing
         * except with mapPartitionsWithIndex you can track which partition is being processed
         *
         * lets say you 12 xml elements in rdd distributed across 4 partition
         * with map + foreach - parser instance will be created 12 time
         * with mapPartitionsWithIndex / mapPartitions - it will be created 4 times.
         *
         * Hence below is not the right solution
         */
        JavaRDD<String> headerRemovedFromRdd1 = rdd1.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true);

        //headerRemovedFromRdd1.foreach(s -> System.out.println(s));

        System.out.println("-------------------------------------------------------------------------------");

        JavaRDD<String> repartition = rdd1.repartition(10);

        JavaRDD<String> rdd = repartition.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        },true);

        rdd.foreach(s -> System.out.println(s));
    }

}
