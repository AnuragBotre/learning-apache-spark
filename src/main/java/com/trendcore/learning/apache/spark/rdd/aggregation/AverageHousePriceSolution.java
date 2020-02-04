package com.trendcore.learning.apache.spark.rdd.aggregation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

/**
 * Create a Spark program to read the house data from in/RealEstate.csv,
 * output the average price for houses with different number of bedrooms.
 * <p>
 * The houses dataset contains a collection of recent real estate listings in 
 * San Luis Obispo county and
 * around it. 
 * <p>
 * The dataset contains the following fields:
 * 1. MLS: Multiple listing service number for the house (unique ID).
 * 2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
 * northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
 * some out of area locations as well.
 * 3. Price: the most recent listing price of the house (in dollars).
 * 4. Bedrooms: number of bedrooms.
 * 5. Bathrooms: number of bathrooms.
 * 6. Size: size of the house in square feet.
 * 7. Price/SQ.ft: price of the house per square foot.
 * 8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.
 * <p>
 * Each field is comma separated.
 * <p>
 * Sample output:
 * <p>
 * (3, 325000)
 * (1, 266356)
 * (2, 325000)
 * ...
 * <p>
 * 3, 1 and 2 mean the number of bedrooms.
 * 325000 means the average price of houses with 3 bedrooms is 325000.
 */
public class AverageHousePriceSolution {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("AverageHousePriceSolution")
                .setMaster("local[3]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("in/RealEstate.csv");

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, false).mapToPair(s -> {
            String[] split = s.split(",");
            String noOfBedRooms = split[3];
            double price = Double.parseDouble(split[2]);
            return new Tuple2<>(noOfBedRooms, price);
        });

        Map<String, Long> stringLongMap = stringDoubleJavaPairRDD.countByKey();

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD1 = stringDoubleJavaPairRDD.reduceByKey((v1, v2) -> v1 + v2);

        stringDoubleJavaPairRDD1.mapToPair(stringDoubleTuple2 -> {
            Long aLong = stringLongMap.getOrDefault(stringDoubleTuple2._1,1L);
            Double aDouble = stringDoubleTuple2._2 / aLong;
            return new Tuple2<>(stringDoubleTuple2._1, aDouble);
        }).foreach(stringDoubleTuple2 -> {
            System.out.println(stringDoubleTuple2._1 + " :: " + stringDoubleTuple2._2);
        });

    }

}
