package com.trendcore.learning.apache.spark;

import com.trendcore.classloader.IntermediateClassLoader;
import org.apache.spark.SparkConf;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class ClassLoadingTest{

    public static void main(String[] args) throws Exception {
        ClassLoader parentClassLoader = ClassLoadingTest.class.getClassLoader();
        URL url = ClassLoadingTest.class.getProtectionDomain().getCodeSource().getLocation()
                .toURI().toURL();
        InputStream skipPackagesPropertiesFile = parentClassLoader.getResourceAsStream("skip-packages.properties");
        Properties properties = new Properties();
        properties.load(skipPackagesPropertiesFile);

        IntermediateClassLoader classLoader = new IntermediateClassLoader(url,parentClassLoader, properties);
        classLoader.invoke(ClassLoadingTest.class.getName(),args);
    }

    public static void mainMethod(String[] args) {
        System.out.println(ClassLoadingMainMethod.class.getClassLoader());
        SparkConf wordCountSparkConf = new SparkConf().setAppName("WordCount").setMaster("local[3]");
        System.out.println(wordCountSparkConf.getClass().getClassLoader());
    }

}
