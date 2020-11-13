package com.trendcore.learning.apache.spark;

import com.trendcore.learning.apache.spark.rdd.SparkMasterRDD;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class SparkLauncherApp {

    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher
            //.setSparkHome("/home/anurag/Apache-Spark/spark-2.4.7-bin-hadoop2.7")
            .setAppResource("apache-spark-1.0-SNAPSHOT.jar")
            //.addJar("/home/anurag/IdeaProjects/learning-apache-spark/target/apache-spark-1.0-SNAPSHOT.jar")
            .setMainClass(SparkMasterRDD.class.getName())
            .setMaster("spark://192.168.0.104:7077")
            .setAppName("First Spark Submit Application")
            .setDeployMode("cluster")
            .setVerbose(true);

        Process launch = sparkLauncher.launch();




    }

}
