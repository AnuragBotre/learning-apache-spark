package com.trendcore.learning.apache.spark.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRDDFromRegularRDD {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                                .setAppName("PairRDDFromRegularRDD")
                                .setMaster("local[1]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");

        JavaRDD<Tuple2> coalesce = sparkContext.parallelize(inputStrings)
                .map(v1 -> {
                    String[] s = v1.split(" ");
                    return new Tuple2(s[0], s[1]);
                }).coalesce(2);

        System.out.println(coalesce.getNumPartitions());

        coalesce.partitions().forEach(partition -> {
            System.out.println(partition.hashCode() + " " + partition.index());
        });

        coalesce.
                saveAsTextFile("output/pair_rdd_from_regular_rdd");

    }

}
