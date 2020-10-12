package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import scala.Option;
import scala.collection.Seq;
import scala.collection.Traversable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.mutable.Builder;

public class ExtendedUserDefinedFunction extends UserDefinedFunction {

    public ExtendedUserDefinedFunction(Object f, DataType dataType, Option<Seq<DataType>> inputTypes) {
        super(f, dataType, inputTypes);
    }

    @Override
    public Column apply(Seq<Column> exprs) {
        Column test = new Column(new Expression() {
            @Override
            public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
                return null;
            }

            @Override
            public Object eval(InternalRow input) {
                return null;
            }

            @Override
            public DataType dataType() {
                return null;
            }

            @Override
            public boolean nullable() {
                return false;
            }

            @Override
            public Seq<Expression> children() {
                return null;
            }

            @Override
            public Object productElement(int n) {
                return null;
            }

            @Override
            public int productArity() {
                return 0;
            }

            @Override
            public boolean canEqual(Object that) {
                return false;
            }
        });
        return test;
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }
}
