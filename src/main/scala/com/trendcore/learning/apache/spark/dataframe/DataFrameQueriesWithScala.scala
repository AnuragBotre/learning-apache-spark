package com.trendcore.learning.apache.spark.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameQueriesWithScala {

  /**
   * case class is immutable in scala.
   * In case of clone provides shallow copy.
   */
  case class Person(first_name: String, age: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate();

    val context = sparkSession.sqlContext;

    /*This line is important. Only this line will give us toDF() method.*/
    import context.implicits._
    /*Create data frame*/
    val personDF = sparkSession.sparkContext.parallelize(List(Person("1", 20), Person("2", 22))).toDF();

    /*Register Temporary view. Lifetime of this view is tied to SparkSession.
    If we want our table or structured to be stored and used across session then use
    dataframe.createOrReplaceGlobalTempView("someTable")
    */
    personDF.createOrReplaceTempView("Person");

    /*Execute query with where clause.*/
    val frame = sparkSession.sql("SELECT p.first_name , p.age from Person p where p.first_name = '1' ");
    frame.show();

    /*Execute query with group by clause.*/
    val frame1 = sparkSession.sql("SELECT p.first_name , count(p.age) as cnt  from Person p group by p.first_name ");
    frame1.show();

    val frame2 = sparkSession.sql("SELECT p.first_name , p.age  from Person p ");
    frame2.show();

    val frame3 = personDF.select("first_name").groupBy("first_name").count();
    frame3.show();

    System.out.println("--------------------------------------------------");
    val frame4 = personDF.select("first_name", "age").groupBy("first_name").count();
    frame4.show();

    val frame5 = personDF.select("first_name", "age").groupBy("first_name", "age").count();
    frame5.show();

    System.out.println("-------------------------------------------------- Frame - 6 ");

    /*
      This will give access to implicit sql functions.
    */
    import org.apache.spark.sql.functions._

    val frame6 = personDF.select("first_name", "age")
      .groupBy("first_name")
      .agg(sum("age"), count("age"));
    frame6.show();

    val frame7 = personDF.select("first_name", "age")
      .groupBy("first_name")
      .agg(sum("age").alias("sum_age"), count("age").alias("count_age"));
    frame7.show();
  }

  private def printResult(frame: DataFrame) = {
    /*Collect result.*/
    val rows = frame.collect();

    println("Printing result.")

    /*Iterate over result.*/
    for (elem <- rows) {
      println(elem);
    }
  }
}
