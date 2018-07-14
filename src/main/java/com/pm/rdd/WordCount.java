package com.pm.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("Word Count").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = javaSparkContext.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        Map<String,Long> wordCount = words.countByValue();
        wordCount.forEach((k,v) -> System.out.println(k + ":" + v));
    }
}
