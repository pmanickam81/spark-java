package com.pm.sparksql;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.log4j.Logger.getLogger;
import static org.apache.spark.sql.functions.*;
public class HousingPrice {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\hadoop");
        getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("HousingPrice").master("local[1]").getOrCreate();
        Dataset<Row> realEstate = session.read().option("header","true").csv("in/RealEstate.csv");
        System.out.println("Schema for this file is");
        realEstate.printSchema();
        System.out.println("Casting the attributes data types to Long");
        Dataset<Row> castedRealEstate = realEstate.withColumn(PRICE, col(PRICE).cast("long"))
                                                    .withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast("long"));
        castedRealEstate.printSchema();
        castedRealEstate.groupBy("Location").count().show();
        castedRealEstate.groupBy("Location")
                .agg(avg(PRICE_SQ_FT), max(PRICE))
                .orderBy(col("avg(" + PRICE_SQ_FT + ")").desc())
                .show();
    }

}
