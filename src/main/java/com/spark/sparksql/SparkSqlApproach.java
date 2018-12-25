package com.spark.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSqlApproach {
    public static void main(String[] args) {
        //SparkSession in Spark 2.0 provides builtin support for Hive features including the ability to write
        // queries using HiveQL, access to Hive UDFs, and the ability to read data from Hive tables.
        // To use these features, you do not need to have an existing Hive setup.

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark Sql Demo")
                .config("spark.master", "local")
                .getOrCreate();

        System.out.println("-----------Creating dataframe-----------");
        Dataset<Row> df = sparkSession.read().json("src/main/resources/employee.json"); //Single line mode
        df.printSchema();
        df.show();

        System.out.println("----------Untyped Dataset Operations (aka DataFrame Operations)------------");
        // Select only the "name" column
        df.select("name").show();
        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show(); //col is import static : // col("...") is preferable to df.col("...")
        // Select people older than 20
        df.select(col("age").gt(20)).show();
        //Count people by age
        df.groupBy("age").count().show();

        System.out.println("----------Running SQL Queries Programmatically-------------");
        df.createOrReplaceTempView("employees");
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM employees");
        sqlDF.show();

        sparkSession.stop();
    }
}
