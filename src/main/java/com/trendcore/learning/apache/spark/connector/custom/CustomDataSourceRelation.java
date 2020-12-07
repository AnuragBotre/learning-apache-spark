package com.trendcore.learning.apache.spark.connector.custom;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

public class CustomDataSourceRelation extends BaseRelation implements PrunedScan {

    private final SQLContext sqlContext;

    public CustomDataSourceRelation(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    /**
     * This method provides schema for the relation or table.
     * @return
     */
    @Override
    public StructType schema() {
        return new StructType()
                .add("id", DataTypes.LongType, true, Metadata.empty())
                .add("name", DataTypes.StringType, true, Metadata.empty());
    }

    /**
     * Scan from the source system.
     * Provide RDD for the table.
     * @param requiredColumns
     * @return
     */
    @Override
    public RDD<Row> buildScan(String[] requiredColumns) {
        //this will return rdd
        List<Row> list = new ArrayList<>();
        /*Here in production scenario connection to database will be made*/
        list.add(new GenericRow(new Object[]{1L,"Adam"}));
        list.add(new GenericRow(new Object[]{2L,"John"}));
        list.add(new GenericRow(new Object[]{3L,"Alex"}));
        list.add(new GenericRow(new Object[]{4L,"Ash"}));

        RDD<Row> rowRDD = ToRDD.toRdd(sqlContext.sparkContext(), list,ClassTag.apply(Row.class));
        return rowRDD;
    }

    /**
     * Returns the list of [[Filter]]s that this datasource
     * may not be able to handle.
     * In first case we will return the array itself
     * stating that it is not able to
     * handle any filters.
     * The returned filter in the array will be handled by Spark SQL.
     * @param filters
     * @return
     */
    @Override
    public Filter[] unhandledFilters(Filter[] filters) {
        return super.unhandledFilters(filters);
    }

}
