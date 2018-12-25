package com.spark.spark1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark1Approach {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark App").setMaster("local"); //that contains information about your application.
        JavaSparkContext sc = new JavaSparkContext(conf); // which tells Spark how to access a cluster

        //Parallelized Collections
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

    }
}
