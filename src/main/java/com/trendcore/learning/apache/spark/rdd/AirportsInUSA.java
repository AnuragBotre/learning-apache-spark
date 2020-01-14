package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Create a Spark program to read the airport data from in/airports.text,
 * find all the airports which are located in United States
 * and output the airport's name and the city's name to out/airports_in_usa.text.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * "Putnam County Airport", "Greencastle"
 * "Dowagiac Municipal Airport", "Dowagiac"
 * ...
 */
public class AirportsInUSA {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUSA").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("in/airports.text");

        rdd.map(v1 -> v1.split(","))
                .filter(v1 -> {
                    //System.out.println(v1[3]+":"+"\"United States\"".toLowerCase().equals(v1[3].trim().toLowerCase()));
                    return "\"United States\"".equals(v1[3]);
                })
                .map(v1 -> new Tuple2(v1[1], v1[2]))
                .saveAsTextFile("output/airports_in_usa.text");
                //.collect()
                //.forEach(t -> System.out.println(t._1 + ":" + t._2));
    }

}
