package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
 * Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * "St Anthony", 51.391944
 * "Tofino", 49.082222
 * ...
 */
public class AirportsByLatitude {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AirportsByLatitude").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("in/airports.text");

        stringJavaRDD
                .map(v1 -> v1.split(","))
                .filter(v1 -> getLatitude(v1[6]) > 40)
                .map(v1 -> new Tuple2(v1[1], v1[6]))
                .saveAsTextFile("output/airports_by_latitude.text");
                //.foreach(tuple2 -> System.out.println(tuple2._1 + " " + tuple2._2));

    }

    private static float getLatitude(String s) {
        try {
            return Float.parseFloat(s);
        } catch (Exception e) {
            return 0;
        }
    }

}
