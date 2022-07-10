package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession =
                SparkSession.builder()
                        .master("local")
                        .appName("Spark Test Application")
                        .config("spark.deploy.mode", "cluster")
                        .getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");

        Dataset<Row> dataset = sparkSession.read().format("csv").option("header", true).load("C:\\Users\\Nikhil\\Downloads\\data\\");

        dataset.show(false);
        System.out.println("Total Records : "+dataset.count());
    }
}