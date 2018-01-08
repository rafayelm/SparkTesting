package org.sparkexample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJdbcTest {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        runJdbcDatasetExample(spark);
        spark.stop();
    }

    private static void runJdbcDatasetExample(SparkSession spark) {

        Dataset<Row> geolocationDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/geolocation.csv");

        geolocationDf.show();

        geolocationDf.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3308/testDB")
                .option("dbtable", "geolocation2")
                .option("user", "root")
                .option("password", "password")
                .save();


        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3308/testDB")
                .option("dbtable", "geolocation2")
                .option("user", "root")
                .option("password", "password")
                .load();

        //filtering data
        Dataset<Row> jdbcDF2 = jdbcDF.filter("event = 'overspeed' or event = 'unsafe following distance'");
        // Saving data to a JDBC source
        jdbcDF2.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3308/testDB")
                .option("dbtable", "Test1")
                .option("user", "root")
                .option("password", "password")
                .save();

        //filtering data
        Dataset<Row> jdbcDF3 = jdbcDF.filter("event = 'normal'");
        // Adding data to already existing table via JDBC

        jdbcDF3.write()
                .mode("append")
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3308/testDB")
                .option("dbtable", "Test1")
                .option("user", "root")
                .option("password", "password")
                .save();
    }
}