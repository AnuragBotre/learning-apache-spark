package com.trendcore.learning.apache.spark.dataframe

import org.apache.spark.sql.{Column, SparkSession}



class StringToColumnDelegator(session:SparkSession) {

  import session.sqlContext.implicits._

  def to(s: String) : Column = {
    $"$s";
  }

}
