package com.pm.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UpperCase {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("UpperCase").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lowerCaseLines = javaSparkContext.textFile("in/uppercase.text");
        JavaRDD<String> upperCaseLines = lowerCaseLines.map(String::toUpperCase);
        upperCaseLines.saveAsTextFile("out/uppercase_capital");

    }

}
