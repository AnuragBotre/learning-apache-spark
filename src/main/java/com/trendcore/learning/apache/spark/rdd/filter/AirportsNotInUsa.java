package com.trendcore.learning.apache.spark.rdd.filter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Create a Spark program to read the airport data from in/airports.text;
 * generate a pair RDD with airport name being the key and country name being the value.
 * Then remove all the airports which are located in United States and
 * output the pair RDD to out/airports_not_in_usa_pair_rdd.text
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport, Country where airport is located,
 * IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * <p>
 * ("Kamloops", "Canada")
 * ("Wewak Intl", "Papua New Guinea")
 */
public class AirportsNotInUsa {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                                .setAppName("AirportNotInUSA")
                                .setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sparkContext.textFile("in/airports.text");

        JavaRDD<Tuple2> notInUnitedStates = rdd.map(v1 -> {
            String[] s = v1.split(",");
            return new Tuple2(s[1], s[3]);
        }).filter(v1 -> !v1._2.equals("\"United States\""));

        notInUnitedStates.foreach(tuple2 -> System.out.println(tuple2._1 + " " + tuple2._2));
    }

}
