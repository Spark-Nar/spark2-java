package com.spark.spark1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark1Approach {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark App").setMaster("local"); //that contains information about your application.
        JavaSparkContext sc = new JavaSparkContext(conf); // which tells Spark how to access a cluster

        //Parallelized Collections
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        //External Dataset / RDD basics
        JavaRDD<String> distFile = sc.textFile("F:\\Tutorials\\BigData\\Spark\\TextFileForSpark\\sparkReadTest.txt");
        JavaRDD<Integer> lineLengths = distFile.map(s -> s.length());
        //If we also wanted to use lineLengths again later, we could add:
        lineLengths.persist(StorageLevel.MEMORY_ONLY());

        int size = lineLengths.reduce((a, b) -> a + b);
        System.out.println("File size is "+size);

        //Other methods for directory
        //textFile("/my/directory"), textFile("/my/directory/*.txt"), and textFile("/my/directory/*.gz").

        //Working with key value pair
        JavaRDD<String> lines = sc.textFile("F:\\Tutorials\\BigData\\Spark\\TextFileForSpark\\sparkReadTest.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        sc.close();

    }
}
