package com.trendcore.learning.apache.spark.connector.custom;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

/**
 * This is custom relation provider which provides data for
 * actors table - hard coded for now.
 * In reality the schema needs to retrieved from
 * underlying database / system.
 *
 * In Apache Spark terminology Relation means Table
 * and column means attributes.
 *
 * The class which implements DataSourceRegister interface
 * should also implements
 * RelationProvider or SchemaRelationProvider or
 */
public class CustomDataSourceRegister
        implements DataSourceRegister , RelationProvider {

    @Override
    public String shortName() {
        return "myCustomDataSource";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       Map<String, String> parameters) {

        return new CustomDataSourceRelation(sqlContext);
    }
}
