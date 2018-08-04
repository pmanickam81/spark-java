package com.pm.sparksql;

import org.apache.log4j.Level;
import org.apache.spark.sql.*;

import static org.apache.log4j.Logger.getLogger;
import static org.apache.spark.sql.functions.col;

public class SparkSqlDemo {

    private static final String AGE_MIDPOINT = "age_midpoint";
    private static final String SALARY_MIDPOINT = "salary_midpoint";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\hadoop");
        getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("SQL Demo").master("local[1]").getOrCreate();
        DataFrameReader reader = session.read();
        Dataset<Row> responses = reader.option("header","true").csv("in/2016-stack-overflow-survey-responses.csv");

        System.out.println("Print Schema");
        responses.printSchema();

        System.out.println("Print 10 Records");
        responses.show(10);

        System.out.println("Print some specific columns from the data set");
        responses.select(col("country"), col("self_identification")).show();

        System.out.println("Print specific columns with equals condition");
        responses.select(col("country").equalTo("Afghanistan")).show();

        System.out.println("Print the count of the attributes");
        RelationalGroupedDataset groupedDataSet = responses.groupBy(col("Occupation"));
        groupedDataSet.count().show();

        System.out.println("Casting the response to other data type");
        Dataset<Row> castedResponse = responses.withColumn(SALARY_MIDPOINT, col("salary_midpoint").cast("integer"))
                .withColumn(AGE_MIDPOINT, col("age_midpoint").cast("integer"));
        castedResponse.printSchema();

        System.out.println("Apply filter condition");
        castedResponse.filter(col(AGE_MIDPOINT).$less(20)).show();

        session.stop();
    }

}
