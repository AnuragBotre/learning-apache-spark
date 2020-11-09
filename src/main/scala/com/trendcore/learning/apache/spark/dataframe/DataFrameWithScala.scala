package com.trendcore.learning.apache.spark.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameWithScala {

  /**
   * case class is immutable in scala.
   */
  case class Person(first_name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").getOrCreate();

    val context = sparkSession.sqlContext;

    /*This line is important. Only this line will give us toDF() method.*/
    import context.implicits._

    val personDF = sparkSession.sparkContext.parallelize(List(Person("1", 1), Person("2", 2))).toDF();

    /*Need to work on group by clause queries*/
    val res = personDF.select("first_name", "age").where("age == 1").collect();

    println("result")

    res.foreach(f => println(f))
  }

}
