package com.pm.rdd;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportByLatitude {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("AirportByLatitude").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> airports = javaSparkContext.textFile("in/airports.text");
        JavaRDD<String> airportsInUSA = airports.filter(line -> Float.valueOf(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[6]) > 40);
        JavaRDD<String> airportByCity = airportsInUSA.map(line -> {
            String[] splits = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            return StringUtils.join(new String[]{splits[1],splits[6]}, ",");
        });
        airportByCity.saveAsTextFile("out/airports_by_latitude");

    }

}
